# Paste this into a Custom Code component's Code tab
# Talk to Data Pipeline — unified 5-stage NL-to-SQL pipeline
# Exposed as a Tool for Worker Node via tool_mode=True
#
# Stages:
#   1. Query Analyzer    (CODE — normalize, classify intent, resolve aliases)
#   2. Schema Linker     (LLM  — resolve NL terms to DB columns)
#   3. Context Builder   (CODE — filter knowledge, select examples, assemble prompt)
#   4. SQL Generator     (LLM  — template match or LLM generation)
#   5. SQL Processor     (CODE + DB — validate, fix, execute, format)

from agentcore.custom import Node
import json
import re
import time

# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1 CONSTANTS — Query Analyzer
# ═══════════════════════════════════════════════════════════════════════════════

ABBREVIATIONS = {
    "ytd": "year to date", "yoy": "year over year", "mom": "month over month",
    "qty": "quantity", "amt": "amount", "avg": "average",
    "mfg": "manufacturing", "mgmt": "management", "dept": "department",
    "org": "organization", "fy": "fiscal year",
    "q1": "quarter 1", "q2": "quarter 2", "q3": "quarter 3", "q4": "quarter 4",
}

FILLER_RE = re.compile(
    r"\b(please|can you|could you|show me|i want to see|i need|"
    r"i would like|tell me|give me|display|find me|help me)\b",
    re.IGNORECASE,
)

INTENT_PATTERNS = {
    "enumerate": [r"\blist\b", r"\bshow all\b", r"\bwhat are\b", r"\bwhich\b", r"\bdistinct\b"],
    "top_n": [r"\btop\s+\d+\b", r"\bhighest\s+\d+\b", r"\blargest\s+\d+\b", r"\bbiggest\s+\d+\b"],
    "bottom_n": [r"\bbottom\s+\d+\b", r"\blowest\s+\d+\b", r"\bsmallest\s+\d+\b"],
    "count": [r"\bhow many\b", r"\bnumber of\b", r"\bcount\b"],
    "average": [r"\baverage\b", r"\bavg\b", r"\bmean\b"],
    "time_series": [r"\bmonthly\b", r"\bquarterly\b", r"\bweekly\b", r"\bdaily\b", r"\bby month\b"],
    "trend": [r"\btrend\b", r"\bover time\b", r"\byear over year\b", r"\bgrowth\b"],
    "comparison": [r"\bcompare\b", r"\bvs\b", r"\bversus\b", r"\bdifference\b"],
    "aggregation_grouped": [r"\bby\s+\w+\b", r"\bper\s+\w+\b", r"\bfor each\b"],
    "aggregation": [r"\btotal\b", r"\bsum\b", r"\boverall\b", r"\bspend\b"],
    "filter": [r"\bexcluding\b", r"\bexcept\b", r"\bwithout\b", r"\babove\b", r"\bbelow\b"],
}

SPECIFICITY = {
    "top_n": 10, "bottom_n": 10, "comparison": 8, "trend": 8,
    "time_series": 7, "average": 6, "count": 6, "enumerate": 6,
    "aggregation_grouped": 5, "aggregation": 4, "filter": 3,
}


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT
# ═══════════════════════════════════════════════════════════════════════════════

class CodeEditorNode(Node):
    display_name = "Talk to Data Pipeline"
    description = "Enterprise NL-to-SQL pipeline: analyzes query, links schema, builds context, generates SQL, executes and formats results. Use this tool when the user asks a data question."
    icon = "database"
    name = "TalkToDataPipeline"

    inputs = [
        MessageTextInput(
            name="input_value",
            display_name="User Question",
            info="The natural language question about your data.",
            tool_mode=True,
        ),
        HandleInput(
            name="knowledge_context",
            display_name="Knowledge Context",
            input_types=["Data"],
            info="From Knowledge Processor (unified knowledge dict).",
            required=False,
        ),
        HandleInput(
            name="db_connection",
            display_name="Database Connection",
            input_types=["Data"],
            info="From Database Connector (host, port, credentials, schema DDL).",
            required=True,
        ),
        HandleInput(
            name="llm",
            display_name="Language Model",
            input_types=["LanguageModel"],
            info="LLM for Schema Linking and SQL Generation.",
            required=True,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Result Rows",
            value=100,
            info="Row limit for query results.",
        ),
        IntInput(
            name="query_timeout",
            display_name="Query Timeout (seconds)",
            value=30,
        ),
        MultilineInput(
            name="mandatory_filter",
            display_name="Mandatory Date Filter",
            value="INVOICE_DATE > DATE '2024-04-01'",
            info="Auto-injected WHERE clause. Leave empty to disable.",
        ),
        BoolInput(
            name="enable_templates",
            display_name="Enable Template Matching",
            value=True,
            info="Try SQL templates before calling LLM for generation.",
        ),
        DropdownInput(
            name="sql_dialect",
            display_name="SQL Dialect",
            options=["auto", "oracle", "postgresql", "sqlserver"],
            value="auto",
            info="Auto-detected from DB connector if set to auto.",
        ),
        MultilineInput(
            name="extra_rules",
            display_name="Extra SQL Rules",
            value="",
            info="Additional rules appended to the LLM prompt.",
        ),
        IntInput(
            name="max_examples",
            display_name="Max Few-Shot Examples",
            value=3,
        ),
        BoolInput(
            name="dedup_subquery",
            display_name="Dedup View (GROUP BY ALL)",
            value=True,
            info="Wrap FROM <view> with a subquery that GROUP BY all columns to eliminate duplicate rows.",
        ),
    ]

    outputs = [
        Output(display_name="Results", name="output", method="build_output"),
    ]

    # ───────────────────────────────────────────────────────────────────────
    # MAIN ENTRY POINT
    # ───────────────────────────────────────────────────────────────────────

    def build_output(self) -> Message:
        raw_query = self.input_value or ""
        if raw_query.startswith('"') and raw_query.endswith('"'):
            raw_query = raw_query[1:-1]
        if not raw_query.strip():
            return Message(text="No query provided.")

        # Extract knowledge context
        kc = self.knowledge_context
        knowledge = {}
        if kc and kc != "":
            knowledge = kc.data if isinstance(kc, Data) else (kc if isinstance(kc, dict) else {})

        # Extract DB connection
        db = self.db_connection
        db_data = db.data if isinstance(db, Data) else (db if isinstance(db, dict) else {})
        if not db_data:
            return Message(text="Error: No database connection provided.")

        provider = self.sql_dialect if self.sql_dialect != "auto" else db_data.get("provider", "postgresql")
        schema_ddl = db_data.get("schema_ddl", "")

        try:
            # STAGE 1: Query Analyzer
            stage1 = self._stage1_query_analyzer(raw_query, knowledge)

            # STAGE 2: Schema Linker
            stage2 = self._stage2_schema_linker(stage1, knowledge, db_data)

            # STAGE 3: Context Builder
            stage3 = self._stage3_context_builder(stage2, knowledge, provider, schema_ddl)

            # STAGE 4: SQL Generator
            stage4 = self._stage4_sql_generator(stage3, knowledge)

            # STAGE 5: SQL Processor
            result_msg = self._stage5_sql_processor(stage4, knowledge, db_data, provider, schema_ddl)

            return result_msg

        except Exception as e:
            return Message(text=f"Pipeline error: {e}")

    # ───────────────────────────────────────────────────────────────────────
    # STAGE 1: Query Analyzer (CODE)
    # ───────────────────────────────────────────────────────────────────────

    def _stage1_query_analyzer(self, raw, knowledge):
        text = raw.strip()
        expansions = []
        for abbr, full in ABBREVIATIONS.items():
            pat = re.compile(r"\b" + re.escape(abbr) + r"\b", re.IGNORECASE)
            if pat.search(text):
                text = pat.sub(full, text)
                expansions.append(f"{abbr} -> {full}")

        alias_resolutions = []
        entity_aliases = knowledge.get("entity_aliases", {})
        if entity_aliases:
            text_lower = text.lower()
            for alias in sorted(entity_aliases.keys(), key=len, reverse=True):
                # Word-boundary match to avoid false positives (e.g. "esp" matching "responsible")
                if re.search(r"\b" + re.escape(alias) + r"\b", text_lower):
                    info = entity_aliases[alias]
                    alias_resolutions.append({
                        "alias": alias,
                        "canonical_value": info.get("canonical_value", ""),
                        "sql_filter": info.get("sql_filter", ""),
                    })

        extracted_numbers = [int(m) for m in re.findall(r"\b(\d+)\b", text)]

        cleaned = FILLER_RE.sub("", text)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
        if len(cleaned) < 3:
            cleaned = text

        # Intent classification
        query_lower = cleaned.lower()
        query_tokens = {w.strip(".,?!'\"") for w in query_lower.split() if len(w.strip(".,?!'\"")) > 1}

        scores = []
        matched_phrases = []

        intent_index = knowledge.get("intent_index", {})
        for intent_name, intent_data in intent_index.items():
            overlap = len(query_tokens & intent_data.get("tokens", set()))
            if overlap > 0:
                score = overlap / max(len(intent_data.get("tokens", set())), 1)
                scores.append([intent_name, score])

        for intent_name, patterns in INTENT_PATTERNS.items():
            for pat in patterns:
                m = re.search(pat, query_lower)
                if m:
                    matched_phrases.append(f"{intent_name}: {m.group()}")
                    found = False
                    for i, (name, score) in enumerate(scores):
                        if name == intent_name:
                            scores[i][1] = min(score + 0.3, 1.0)
                            found = True
                            break
                    if not found:
                        scores.append([intent_name, 0.4])
                    break

        for i, (name, score) in enumerate(scores):
            scores[i][1] = score + SPECIFICITY.get(name, 0) * 0.03
        scores.sort(key=lambda x: x[1], reverse=True)

        if scores:
            confidence = round(min(scores[0][1], 1.0), 3)
            intent = {
                "primary_intent": scores[0][0],
                "secondary_intents": [s[0] for s in scores[1:4]],
                "confidence": confidence,
                "confidence_level": "high" if confidence >= 0.6 else ("medium" if confidence >= 0.3 else "low"),
                "matched_phrases": matched_phrases,
            }
        else:
            intent = {"primary_intent": "unknown", "secondary_intents": [], "confidence": 0.0, "confidence_level": "low", "matched_phrases": []}

        return {
            "raw_query": raw,
            "normalized_query": cleaned,
            "normalizer": {
                "expansions": expansions,
                "alias_resolutions": alias_resolutions,
                "extracted_numbers": extracted_numbers,
            },
            "intent": intent,
        }

    # ───────────────────────────────────────────────────────────────────────
    # STAGE 2: Schema Linker (LLM)
    # ───────────────────────────────────────────────────────────────────────

    def _stage2_schema_linker(self, ctx, knowledge, db_data):
        normalized_query = ctx.get("normalized_query", ctx.get("raw_query", ""))
        alias_resolutions = ctx.get("normalizer", {}).get("alias_resolutions", [])

        synonym_map = knowledge.get("synonym_map", {})
        entities = knowledge.get("entities", {})
        col_hints = knowledge.get("column_value_hints", {})

        syn_lines = [f'  "{t}" -> {info.get("column", "?")}' for t, info in list(synonym_map.items())[:100]]
        ent_lines = [f"  {n}: PK={info.get('primary_key','?')}, Display={info.get('display_column','?')}" for n, info in entities.items()]
        hint_lines = [f"  {col}: {', '.join(str(v) for v in h.get('examples',[])[:8])}" for col, h in col_hints.items() if h.get("examples")]

        alias_section = ""
        if alias_resolutions:
            alias_section = "\nALIAS RESOLUTIONS:\n" + "\n".join(f"  {a['alias']} -> {a['sql_filter']}" for a in alias_resolutions)

        prompt = f"""You are a schema linking agent. Resolve natural language terms to database column names.

SYNONYM MAP:
{chr(10).join(syn_lines) if syn_lines else '  (none)'}

ENTITIES:
{chr(10).join(ent_lines) if ent_lines else '  (none)'}

COLUMN VALUE EXAMPLES:
{chr(10).join(hint_lines) if hint_lines else '  (none)'}
{alias_section}

User query: "{normalized_query}"

Respond with JSON:
{{"resolved_columns": {{}}, "detected_entities": [], "suggested_groupby": [], "suggested_filters": [], "suggested_orderby": null, "suggested_limit": null}}

Return ONLY JSON."""

        schema_linking = {}
        try:
            response = self.llm.invoke(prompt)
            text = response.content if hasattr(response, "content") else str(response)
            text = text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            schema_linking = json.loads(text.strip())
        except Exception as e:
            schema_linking = {"error": str(e)}

        # Inject alias-resolved filters
        if alias_resolutions:
            filters = schema_linking.get("suggested_filters", [])
            for a in alias_resolutions:
                sf = a.get("sql_filter", "")
                if sf and sf not in filters:
                    filters.append(sf)
            schema_linking["suggested_filters"] = filters

        return {**ctx, "schema_linking": schema_linking}

    # ───────────────────────────────────────────────────────────────────────
    # STAGE 3: Context Builder (CODE)
    # ───────────────────────────────────────────────────────────────────────

    def _stage3_context_builder(self, ctx, knowledge, provider, schema_ddl):
        schema_linking = ctx.get("schema_linking", {})
        intent_result = ctx.get("intent", {})
        normalized_query = ctx.get("normalized_query", ctx.get("raw_query", ""))
        query_lower = normalized_query.lower()
        mr = self.max_rows

        # Compute resolved columns
        resolved_cols = set()
        for col in schema_linking.get("resolved_columns", {}).values():
            resolved_cols.add(str(col).upper())
        entities = knowledge.get("entities", {})
        for ent_name in schema_linking.get("detected_entities", []):
            for col in entities.get(ent_name, {}).get("columns", []):
                resolved_cols.add(str(col).upper())

        # Smart context filtering
        fk = knowledge
        filtered = {}

        cm = fk.get("column_metadata", {})
        filtered["column_metadata"] = {k: v for k, v in cm.items() if k.upper() in resolved_cols} if resolved_cols else cm

        ch = fk.get("column_value_hints", {})
        filtered["column_value_hints"] = {k: v for k, v in ch.items() if k.upper() in resolved_cols} if resolved_cols else ch

        rules = fk.get("business_rules", {})
        fr = {"exclusion_rules": rules.get("exclusion_rules", []), "oracle_syntax": rules.get("oracle_syntax", {})}
        if any(t in query_lower for t in ("total", "sum", "average", "avg", "count", "spend", "cost", "kpi")):
            fr["metrics"] = rules.get("metrics", {})
        if any(t in query_lower for t in ("year", "month", "quarter", "date", "period", "ytd", "yoy")):
            fr["time_filters"] = rules.get("time_filters", {})
        if any(t in query_lower for t in ("type", "category", "class", "material", "oem", "abc")):
            fr["classification_rules"] = rules.get("classification_rules", {})
        filtered["business_rules"] = fr

        hierarchies = fk.get("hierarchies", {})
        if resolved_cols:
            filtered["hierarchies"] = {
                n: info for n, info in hierarchies.items()
                if {l.get("column", "").upper() for l in info.get("levels", [])} & resolved_cols
            }
        else:
            filtered["hierarchies"] = hierarchies

        filtered["additional_domain_context"] = fk.get("additional_domain_context", "")

        # Example selection
        all_examples = fk.get("examples", [])
        primary_intent = intent_result.get("primary_intent", "")
        detected_ents = schema_linking.get("detected_entities", [])
        entity_set = {e.lower() for e in detected_ents}

        scored = []
        for ex in all_examples:
            score = 0.0
            ql = (ex.get("question") or ex.get("input", "")).lower()
            sl = (ex.get("sql") or ex.get("output", "")).lower()
            tags = {t.lower() for t in ex.get("tags", [])}
            cat = (ex.get("category", "") or "").lower()
            if primary_intent and primary_intent.lower() in (cat or tags or ql):
                score += 3
            for entity in entity_set:
                if entity in ql or entity in sl:
                    score += 2
            scored.append((score, ex))
        scored.sort(key=lambda x: x[0], reverse=True)
        selected_examples = [ex for _, ex in scored[:self.max_examples]]

        # Assemble prompt
        sections = [
            f"You are an expert SQL analyst for {provider.upper()} databases. "
            "Generate a precise SQL query for the question below.",
            f"\n**Database Schema:**\n```sql\n{schema_ddl}\n```",
        ]

        resolved = schema_linking.get("resolved_columns", {})
        if resolved:
            lines = [f'  "{t}" -> {c}' for t, c in resolved.items()]
            sections.append("\n**Resolved Columns:**\n" + "\n".join(lines))
        sug_f = schema_linking.get("suggested_filters", [])
        if sug_f:
            sections.append("Suggested filters: " + ", ".join(str(f) for f in sug_f))
        sug_g = schema_linking.get("suggested_groupby", [])
        if sug_g:
            sections.append("Suggested GROUP BY: " + ", ".join(str(g) for g in sug_g))

        if intent_result.get("primary_intent", "unknown") != "unknown":
            sections.append(f"\n**Intent:** {intent_result['primary_intent']} (confidence: {intent_result.get('confidence', 0)})")

        cd_lines = [f"  {c}: {info.get('description', '')}" for c, info in filtered["column_metadata"].items() if info.get("description")]
        if cd_lines:
            sections.append("\n**Column Descriptions:**\n" + "\n".join(cd_lines))

        cvh = filtered["column_value_hints"]
        if cvh:
            h_lines = [f"  {c} ({h.get('cardinality','?')}): {', '.join(str(v) for v in h.get('examples',[])[:8])}" for c, h in cvh.items() if h.get("examples")]
            if h_lines:
                sections.append("\n**Column Values:**\n" + "\n".join(h_lines))

        metrics = filtered["business_rules"].get("metrics", {})
        if metrics:
            sections.append("\n**KPI Definitions:**\n" + "\n".join(f"  {n}: {e}" for n, e in list(metrics.items())[:15]))

        excl = filtered["business_rules"].get("exclusion_rules", [])
        if excl:
            sections.append("\n**Exclusion Rules:**\n" + "\n".join(f"  - {r}" for r in excl[:10]))

        adc = filtered.get("additional_domain_context", "")
        if adc:
            sections.append(f"\n**Domain Context:**\n{adc}")

        if selected_examples:
            ex_lines = []
            for ex in selected_examples:
                q = ex.get("question") or ex.get("input", "")
                s = ex.get("sql") or ex.get("output", "")
                if q and s:
                    ex_lines.append(f"Q: {q}\nSQL: {s}")
            if ex_lines:
                sections.append("\n**Examples:**\n" + "\n\n".join(ex_lines))

        if provider == "oracle":
            sections.append(f"\n**Oracle SQL Rules:**\n1. Use FETCH FIRST {mr} ROWS ONLY (NEVER LIMIT)\n2. Use SYSDATE, NVL(), UPPER() for case-insensitive\n3. MANDATORY: EVERY query must include date filter: INVOICE_DATE > DATE '2024-04-01'")
        else:
            sections.append(f"\n**SQL Rules:**\nUse LIMIT {mr} to cap results.")

        sections.append(f"\n**User Question:** {normalized_query}")
        sections.append(f"\n**Rules:**\n1. SELECT only\n2. Use exact column names\n3. GROUP BY for aggregations\n4. Return ONLY the SQL, no explanations\n5. For name/text filters (SUPPLIER_NAME, PLANT_NAME, etc.), use UPPER(col) LIKE UPPER('%value%') for partial matching, NOT exact equality\n6. Do NOT add filters the user did not ask for (e.g. do not add REGION filters unless the user mentions a region)")

        if self.extra_rules and self.extra_rules.strip():
            sections.append(self.extra_rules.strip())

        sections.append("\n**SQL Query:**")

        prompt_text = "\n".join(sections)
        token_est = len(prompt_text) // 4

        return {
            **ctx,
            "prompt_text": prompt_text,
            "token_estimate": token_est,
            "selected_examples_count": len(selected_examples),
            "total_examples_count": len(all_examples),
            "provider": provider,
            "schema_ddl": schema_ddl,
        }

    # ───────────────────────────────────────────────────────────────────────
    # STAGE 4: SQL Generator (LLM or Template)
    # ───────────────────────────────────────────────────────────────────────

    def _stage4_sql_generator(self, ctx, knowledge):
        intent = ctx.get("intent", {})
        schema_linking = ctx.get("schema_linking", {})
        prompt_text = ctx.get("prompt_text", "")

        sql = ""
        method = "llm"

        # Try template matching
        if self.enable_templates and knowledge and intent.get("confidence_level") == "high":
            templates = knowledge.get("sql_templates", {})
            if templates:
                tmap = {
                    "enumerate": "enumerate_distinct", "top_n": "top_n_by_spend",
                    "bottom_n": "top_n_by_spend", "aggregation_grouped": "aggregation_grouped",
                    "aggregation": "aggregation_grouped", "time_series": "time_series_monthly",
                    "count": "count_distinct",
                }
                tname = tmap.get(intent.get("primary_intent", ""))
                if tname and tname in templates:
                    tmpl = templates[tname].get("template", "")
                    if tmpl:
                        resolved = schema_linking.get("resolved_columns", {})
                        groupby = schema_linking.get("suggested_groupby", [])
                        filters = schema_linking.get("suggested_filters", [])
                        limit = schema_linking.get("suggested_limit")
                        dim = str(groupby[0]) if groupby else (next(iter(resolved.values()), "") if resolved else "")
                        if dim:
                            where = "WHERE " + " AND ".join(str(f) for f in filters) if filters else ""
                            try:
                                sql = tmpl.format(
                                    column=dim, dimension=dim, dimension1=dim,
                                    dimension2=groupby[1] if len(groupby) > 1 else dim,
                                    where_clause=where, n=limit or 10,
                                    filter_column="", filter_value="",
                                    count_column=dim, alias="COUNT",
                                    columns=f"{dim}, SUM(AMOUNT) AS TOTAL_SPEND",
                                    group_by=f"GROUP BY {dim}", threshold=0,
                                ).strip()
                                method = "template"
                            except (KeyError, IndexError):
                                sql = ""

        # Fall back to LLM
        if not sql:
            if not prompt_text:
                return {**ctx, "generated_sql": "", "generation_method": "none", "error": True, "message": "Empty prompt."}
            try:
                response = self.llm.invoke(prompt_text)
                raw = response.content if hasattr(response, "content") else str(response)
                sql = raw.strip()
                if sql.startswith("```sql"):
                    sql = sql[6:]
                if sql.startswith("```"):
                    sql = sql[3:]
                if sql.endswith("```"):
                    sql = sql[:-3]
                sql = sql.strip()
            except Exception as e:
                return {**ctx, "generated_sql": "", "generation_method": "llm", "error": True, "message": f"LLM failed: {e}"}

        return {**ctx, "generated_sql": sql, "generation_method": method}

    # ───────────────────────────────────────────────────────────────────────
    # DEDUP SUBQUERY WRAPPER
    # ───────────────────────────────────────────────────────────────────────

    # All columns of VW_SPEND_REPORT_VIEW for dedup GROUP BY
    _VIEW_COLUMNS = [
        "REGION", "Incoterms (Supplier Master)", "Terms of payment Supplier",
        "TERMS_OF_PAYMENT_DESCRIPTION", "Parent Supplier", "COUNTRY",
        "SAP Project No", "CUSTOMER", "Main Plant Name", "PLANT_NO",
        "PLANT_NAME", "Main Plant No", "SUPPLIER_NO", "SUPPLIER_NAME",
        "FAHRZEUGTYP", "PURCHASING_AGENT_NAME", "MAIN_ACCOUNT",
        "Main_Account_Description", "INVOICE_NO", "INVOICE_POS_NO",
        "AMOUNT", "PAYMENT_TERM", "EXCH_CURRENCY", "G_JAHR", "EXCH_RATE",
        "QUANTITY", "VCHR_LOC_CURRENCY_AMT", "VCHR_LOC_CURRENCY",
        "INVOICE_DATE", "POSTAL_KEY", "ORDER_NO", "REFERENZBELEG",
        "PURCHASE_ORG", "MATERIAL_TYPE", "OEM_PART_NUMBER", "INGREDIENT",
        "PROJECT_NAME", "ARTICLE_NO", "ARTICLE_DESCRIPTION",
        "Material Group", "MG Description", "COMMODITY",
        "COMMODITY_DESCRIPTION", "OEM", "LIFETIME", "ABCINDICATOR",
        "Com. Supplier", "Com. Desr. Supp.",
    ]

    def _apply_dedup_subquery(self, sql, knowledge, schema_ddl):
        """Wrap FROM <view_name> with a dedup subquery using GROUP BY all columns.

        Transforms:
            SELECT ... FROM VW_SPEND_REPORT_VIEW WHERE ...
        Into:
            SELECT ... FROM (SELECT * FROM VW_SPEND_REPORT_VIEW GROUP BY "col1", "col2", ...) t WHERE ...
        """
        # Find the view name in FROM clause
        from_match = re.search(
            r'\bFROM\s+(\w+)',
            sql,
            re.IGNORECASE
        )
        if not from_match:
            return sql

        view_name = from_match.group(1)
        # Only apply to views (starts with VW_ or V_) — skip subqueries
        if not view_name.upper().startswith(("VW_", "V_")):
            return sql

        # Skip if already wrapped (has a subquery after FROM)
        after_from = sql[from_match.start():]
        if re.match(r'\bFROM\s*\(', after_from, re.IGNORECASE):
            return sql

        # Build the GROUP BY column list with quoting
        col_list = ",\n".join(f'"{c}"' for c in self._VIEW_COLUMNS)

        # Replace FROM <view> with FROM (SELECT * FROM <view> GROUP BY <all_cols>) t
        dedup_subquery = f"(SELECT * FROM {view_name} GROUP BY\n{col_list})"
        new_sql = sql[:from_match.start()] + f"FROM {dedup_subquery} t" + sql[from_match.end():]

        return new_sql

    # ───────────────────────────────────────────────────────────────────────
    # STAGE 5: SQL Processor (CODE + DB)
    # ───────────────────────────────────────────────────────────────────────

    def _stage5_sql_processor(self, ctx, knowledge, db_data, provider, schema_ddl=""):
        if ctx.get("error"):
            return Message(text=f"Error from Stage {ctx.get('generation_method', '?')}: {ctx.get('message', 'Unknown')}")

        sql = ctx.get("generated_sql", "")
        if not sql:
            return Message(text="Error: No SQL generated.")

        mr = self.max_rows
        trace_events = []
        all_fixes = []

        # Dedup subquery: wrap FROM <view> with GROUP BY all columns
        if self.dedup_subquery:
            new_sql = self._apply_dedup_subquery(sql, knowledge, schema_ddl)
            if new_sql != sql:
                sql = new_sql
                all_fixes.append("Wrapped view in dedup GROUP BY subquery")

        # Anti-pattern fixes from knowledge
        anti_patterns = knowledge.get("anti_patterns", [])
        for ap in anti_patterns:
            compiled = ap.get("compiled")
            if not compiled or ap.get("required") or ap.get("forbidden"):
                continue
            if not compiled.search(sql):
                continue
            ap_name = ap.get("name", ap.get("id", "?"))
            lm = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
            if lm and "LIMIT" in ap_name.upper():
                n = lm.group(1)
                sql = re.sub(r"\bLIMIT\s+\d+", f"FETCH FIRST {n} ROWS ONLY", sql, flags=re.IGNORECASE)
                all_fixes.append(f"LIMIT -> FETCH FIRST {n}")
                continue
            tm = re.search(r"\bTOP\s+(\d+)\b", sql, re.IGNORECASE)
            if tm and "TOP" in ap_name.upper():
                n = tm.group(1)
                sql = re.sub(r"\bSELECT\s+TOP\s+\d+\b", "SELECT", sql, flags=re.IGNORECASE)
                if "FETCH FIRST" not in sql.upper():
                    sql = sql.rstrip() + f"\nFETCH FIRST {n} ROWS ONLY"
                all_fixes.append(f"TOP -> FETCH FIRST {n}")
                continue
            if "semicolon" in ap_name.lower():
                sql = sql.rstrip().rstrip(";").rstrip()
                all_fixes.append("Removed semicolon")
                continue

        # Cap absurd FETCH FIRST
        fm = re.search(r"FETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY", sql, re.IGNORECASE)
        if fm and int(fm.group(1)) > mr:
            old_n = fm.group(1)
            sql = re.sub(r"FETCH\s+FIRST\s+\d+\s+ROWS?\s+ONLY", f"FETCH FIRST {mr} ROWS ONLY", sql, flags=re.IGNORECASE)
            all_fixes.append(f"Capped FETCH FIRST {old_n} -> {mr}")

        # Cap absurd LIMIT
        lm2 = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
        if lm2 and int(lm2.group(1)) > mr:
            old_n = lm2.group(1)
            sql = re.sub(r"\bLIMIT\s+\d+", f"LIMIT {mr}", sql, flags=re.IGNORECASE)
            all_fixes.append(f"Capped LIMIT {old_n} -> {mr}")

        # Remove redundant fiscal year filters
        for pat, label in [
            (r"\s*AND\s+TO_CHAR\s*\(\s*INVOICE_DATE\s*,\s*'YYYY'\s*\)\s*=\s*TO_CHAR\s*\(\s*SYSDATE\s*,\s*'YYYY'\s*\)", "TO_CHAR fiscal year"),
            (r"\s*AND\s+EXTRACT\s*\(\s*YEAR\s+FROM\s+INVOICE_DATE\s*\)\s*=\s*EXTRACT\s*\(\s*YEAR\s+FROM\s+SYSDATE\s*\)", "EXTRACT(YEAR) fiscal year"),
        ]:
            if re.search(pat, sql, re.IGNORECASE):
                sql = re.sub(pat, "", sql, flags=re.IGNORECASE)
                all_fixes.append(f"Removed redundant {label} filter")

        # Strip trailing semicolons
        if sql.rstrip().endswith(";"):
            sql = sql.rstrip().rstrip(";").rstrip()
            all_fixes.append("Removed trailing semicolon")

        if all_fixes:
            trace_events.append(f"Fixes: {len(all_fixes)} applied")

        # Mandatory date filter injection
        mf = (self.mandatory_filter or "").strip()
        if mf:
            col_match = re.match(r"(\w+)", mf)
            col_name = col_match.group(1).upper() if col_match else ""
            if col_name and col_name not in sql.upper():
                su = sql.upper()
                if "WHERE" in su:
                    wi = su.index("WHERE") + 5
                    sql = sql[:wi] + f" {mf} AND" + sql[wi:]
                elif "GROUP BY" in su:
                    gi = su.index("GROUP BY")
                    sql = sql[:gi] + f"WHERE {mf}\n" + sql[gi:]
                elif "ORDER BY" in su:
                    oi = su.index("ORDER BY")
                    sql = sql[:oi] + f"WHERE {mf}\n" + sql[oi:]
                else:
                    sql = sql.rstrip() + f"\nWHERE {mf}"
                trace_events.append(f"Injected mandatory filter: {mf}")

        # Safety validation
        sql_stripped = sql.strip().rstrip(";")
        blocked = {"DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE", "EXEC", "MERGE"}
        tokens = sql_stripped.upper().split()
        for kw in blocked:
            if kw in tokens:
                return Message(text=f"Error: Blocked keyword '{kw}' in SQL.\n\n```sql\n{sql}\n```")
        first = tokens[0] if tokens else ""
        if first not in ("SELECT", "WITH"):
            return Message(text=f"Error: Query must start with SELECT. Got: {first}\n\n```sql\n{sql}\n```")

        trace_events.append("Validated OK")

        # Execute SQL
        try:
            sql_exec = sql.strip().rstrip(";").strip()
            start = time.time()
            if provider == "oracle":
                import oracledb
                dsn = oracledb.makedsn(db_data["host"], db_data["port"], service_name=db_data["database_name"])
                conn = oracledb.connect(user=db_data["username"], password=db_data["password"], dsn=dsn)
                conn.call_timeout = self.query_timeout * 1000
            else:
                import psycopg2
                conn = psycopg2.connect(
                    host=db_data["host"], port=db_data["port"],
                    dbname=db_data["database_name"],
                    user=db_data["username"], password=db_data["password"],
                    connect_timeout=15,
                    options=f"-c statement_timeout={self.query_timeout * 1000}",
                )
            try:
                cur = conn.cursor()
                cur.execute(sql_exec)
                columns = [d[0] for d in cur.description] if cur.description else []
                rows = cur.fetchall() if columns else []
                cur.close()
            finally:
                conn.close()
            exec_ms = round((time.time() - start) * 1000, 2)
        except Exception as e:
            return Message(text=f"Error executing SQL: {e}\n\n```sql\n{sql}\n```")

        trace_events.append(f"Executed: {len(rows)} rows in {exec_ms}ms")

        # Post-result checks
        post_warnings = []
        if not rows:
            post_warnings.append("Query returned 0 rows -- filters may be too restrictive.")
        if len(rows) == 1 and len(columns) == 1 and rows[0][0] is None:
            post_warnings.append("Aggregation returned NULL.")

        # Format markdown table
        if columns:
            cw = [len(str(c)) for c in columns]
            for row in rows:
                for i, val in enumerate(row):
                    cw[i] = max(cw[i], len(str(val) if val is not None else "NULL"))
            header = "| " + " | ".join(str(c).ljust(cw[i]) for i, c in enumerate(columns)) + " |"
            sep = "|-" + "-|-".join("-" * w for w in cw) + "-|"
            data_rows = []
            for row in rows:
                cells = [str(v if v is not None else "NULL").ljust(cw[i]) for i, v in enumerate(row)]
                data_rows.append("| " + " | ".join(cells) + " |")
            md_table = "\n".join([header, sep] + data_rows)
        else:
            md_table = "_No data_"

        # Build output message
        parts = [
            f"**Query Results** ({len(rows)} rows, {exec_ms}ms)\n",
            f"**SQL:**\n```sql\n{sql}\n```\n",
            md_table,
        ]

        if all_fixes:
            parts.append("\n**Auto-Fixes:**")
            for f in all_fixes:
                parts.append(f"- {f}")

        if post_warnings:
            parts.append("\n**Data Notes:**")
            for w in post_warnings:
                parts.append(f"- {w}")

        # Raw data JSON for Data Visualizer
        rows_list = [list(r) for r in rows]
        data_json = json.dumps({"columns": columns, "rows": rows_list}, default=str)
        parts.append(f"\n<data_json>{data_json}</data_json>")

        # Pipeline trace
        parts.append("\n---\n**Pipeline Trace**\n")

        raw_q = ctx.get("raw_query", "")
        norm_q = ctx.get("normalized_query", raw_q)
        normalizer = ctx.get("normalizer", {})
        intent = ctx.get("intent", {})
        sl = ctx.get("schema_linking", {})
        gen_method = ctx.get("generation_method", "?")
        tok_est = ctx.get("token_estimate", 0)
        n_ex = ctx.get("selected_examples_count", 0)
        tot_ex = ctx.get("total_examples_count", 0)

        parts.append("**1. Query Analysis** (CODE)")
        if raw_q != norm_q:
            parts.append(f"- Original: `{raw_q}`")
            parts.append(f"- Normalized: `{norm_q}`")
        else:
            parts.append(f"- Query: `{raw_q}`")
        exps = normalizer.get("expansions", [])
        if exps:
            parts.append(f"- Expansions: {', '.join(str(e) for e in exps)}")
        aliases = normalizer.get("alias_resolutions", [])
        for a in aliases:
            parts.append(f"- Alias: `{a.get('alias','')}` -> `{a.get('sql_filter','')}`")
        conf = intent.get("confidence", 0)
        bar_len = int(conf * 20)
        bar = "\u2588" * bar_len + "\u2591" * (20 - bar_len)
        parts.append(f"- Intent: `{intent.get('primary_intent','?')}` ({conf:.0%}) {bar} {intent.get('confidence_level','?').upper()}\n")

        if sl:
            parts.append("**2. Schema Linking** (LLM)")
            for t, c in sl.get("resolved_columns", {}).items():
                parts.append(f"- `{t}` -> `{c}`")
            ents = sl.get("detected_entities", [])
            if ents:
                parts.append(f"- Entities: {', '.join(ents)}")
            parts.append("")

        parts.append(f"**3. Context Building** (CODE)\n- Prompt: ~{tok_est} tokens | Examples: {n_ex}/{tot_ex}\n")
        parts.append(f"**4. SQL Generation** ({gen_method.upper()})")
        if gen_method == "template":
            parts.append("- Template matched -- LLM skipped")
        parts.append("")

        parts.append("**5. Validation & Execution** (CODE + DB)")
        for ev in trace_events:
            parts.append(f"- {ev}")
        parts.append("\n---")

        self.status = f"{len(rows)} rows in {exec_ms}ms | {gen_method}"
        return Message(text="\n".join(parts))
