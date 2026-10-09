class DataQuality:
    """Data quality analysis class for bronze layer tables.

    Usage:
        dq = DataQuality("dataquality_challenge", "bronze", "dim_produto", "/path/to/bronze.json")
        dq.validate()
    """

    def __init__(self, catalog, schema, table, path_to_rules):
        self.base = f"{catalog}.{schema}"
        self.table = table
        self.path_to_rules = path_to_rules

    @property
    def _table(self):
        return f"{self.base}.{self.table}"

    def _count(self):
        return spark.table(self._table).count()

    # =========================================================
    # Analysis methods
    # =========================================================

    def check_nulls(self, column):
        """Checks for null values in the specified column."""
        total = self._count()
        null_count = spark.sql(
            f"SELECT COUNT(*) FROM {self._table} WHERE `{column}` IS NULL"
        ).collect()[0][0]
        pct = round(null_count / total * 100, 1) if total else 0.0
        return {"table": self.table, "column": column, "check": "Nulls", "percent": pct,
                "total_count": total, "fail_count": null_count}

    def check_duplicates(self, columns=None, column=None):
        """Checks for duplicate values in the specified column(s)."""
        cols = columns if columns else [column]
        total = self._count()
        col_list = ", ".join([f"`{c}`" for c in cols])
        dup_count = spark.sql(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT {col_list}, COUNT(*) as cnt FROM {self._table} GROUP BY {col_list} HAVING cnt > 1"
            f") t"
        ).collect()[0][0]
        col_name = " + ".join(cols)
        pct = round(dup_count / total * 100, 1) if total else 0.0
        return {"table": self.table, "column": col_name, "check": "Duplicates", "percent": pct,
                "total_count": total, "fail_count": dup_count}

    def check_referential_integrity(self, column, dim_table, dim_column):
        """Checks if the column values exist in the referenced dimension table."""
        total = self._count()
        orphans = spark.sql(f"""
            SELECT COUNT(*) FROM {self._table} f
            LEFT JOIN {self.base}.{dim_table} d ON f.`{column}` = d.`{dim_column}`
            WHERE d.`{dim_column}` IS NULL
        """).collect()[0][0]
        col_name = f"{column} -> {dim_table}.{dim_column}"
        pct = round(orphans / total * 100, 1) if total else 0.0
        return {"table": self.table, "column": col_name, "check": "Referential Integrity", "percent": pct,
                "total_count": total, "fail_count": orphans}

    def check_negatives(self, column):
        """Checks for negative values in the specified numeric column."""
        total = self._count()
        cnt = spark.sql(f"SELECT COUNT(*) FROM {self._table} WHERE `{column}` < 0").collect()[0][0]
        pct = round(cnt / total * 100, 1) if total else 0.0
        return {"table": self.table, "column": column, "check": "Negatives", "percent": pct,
                "total_count": total, "fail_count": cnt}

    def check_date_range(self, start_column, end_column):
        """Checks if start_column > end_column (invalid date range)."""
        total = self._count()
        cnt = spark.sql(f"SELECT COUNT(*) FROM {self._table} WHERE `{start_column}` > `{end_column}`").collect()[0][0]
        col_name = f"{start_column} > {end_column}"
        pct = round(cnt / total * 100, 1) if total else 0.0
        return {"table": self.table, "column": col_name, "check": "Invalid Date Range", "percent": pct,
                "total_count": total, "fail_count": cnt}

    def check_domain(self, column, expected_values):
        """Checks if the column values are within the expected domain."""
        total = self._count()
        distinct_vals = spark.sql(
            f"SELECT DISTINCT `{column}` FROM {self._table} WHERE `{column}` IS NOT NULL"
        ).collect()
        unexpected = [row[0] for row in distinct_vals if row[0] not in expected_values]
        unexpected_count = spark.sql(
            f"SELECT COUNT(*) FROM {self._table} WHERE `{column}` IS NOT NULL AND `{column}` NOT IN ({', '.join([repr(v) for v in expected_values])})"
        ).collect()[0][0] if unexpected else 0
        pct = round(unexpected_count / total * 100, 1) if total else 0.0
        return {"table": self.table, "column": column, "check": "Unexpected Domain", "percent": pct,
                "total_count": total, "fail_count": unexpected_count}

    def check_empty_strings(self, column):
        """Checks for empty or whitespace-only strings in the specified column."""
        total = self._count()
        cnt = spark.sql(
            f"SELECT COUNT(*) FROM {self._table} WHERE `{column}` IS NOT NULL AND TRIM(`{column}`) = ''"
        ).collect()[0][0]
        pct = round(cnt / total * 100, 1) if total else 0.0
        return {"table": self.table, "column": column, "check": "Empty Strings", "percent": pct,
                "total_count": total, "fail_count": cnt}

    # =========================================================
    # Rule-based validation (silver.json)
    # =========================================================

    def validate(self):
        """Validates all rules for the table defined in the JSON rules file.

        Usage:
            dq = DataQuality('dataquality_challenge', 'bronze', 'dim_loja', '/path/to/bronze.json')
            dq.validate()

        Returns a list of dicts with table, column, check,
        percent, max_percent and status ("PASS" or "FAIL").
        """
        import json

        with open(self.path_to_rules, "r", encoding="utf-8") as f:
            all_rules = json.load(f)

        table_rules = next((t for t in all_rules if t["table"] == self.table), None)
        if not table_rules:
            raise ValueError(f"Table '{self.table}' not found in {self.path_to_rules}")

        results = []
        for col_def in table_rules["columns"]:
            col_name = col_def["name"]
            for rule in col_def["rules"]:
                rule_type = rule["type"]
                max_pct = rule.get("max_percent", 0)
                safe_auto_fix = rule.get("safe_auto_fix", False)

                if rule_type == "nulls":
                    r = self.check_nulls(col_name)
                elif rule_type == "duplicates":
                    r = self.check_duplicates(columns=rule.get("composite_key"), column=col_name)
                elif rule_type == "referential_integrity":
                    r = self.check_referential_integrity(col_name, rule["ref_table"], rule["ref_column"])
                elif rule_type == "negatives":
                    r = self.check_negatives(col_name)
                elif rule_type == "date_range":
                    r = self.check_date_range(col_name, rule["end_column"])
                elif rule_type == "domain":
                    r = self.check_domain(col_name, rule["expected_values"])
                elif rule_type == "empty_string":
                    r = self.check_empty_strings(col_name)
                else:
                    continue

                r["rule_id"] = rule.get("id", "")
                r["description"] = rule.get("description", "")
                r["severity"] = rule.get("severity", "")
                r["max_percent"] = max_pct
                r["status"] = "PASS" if r["percent"] <= max_pct else "FAIL"

                # Classification and treatment (Seção 6 do enunciado)
                if r["percent"] == 0:
                    r["classification"] = "Aprovado"
                    r["treatment"] = "none"
                elif r["percent"] <= max_pct:
                    r["classification"] = "Aprovado com alerta"
                    r["treatment"] = "quarantine"
                else:
                    if safe_auto_fix:
                        r["classification"] = "Corrigido automaticamente"
                        r["treatment"] = "auto_fix"
                    else:
                        r["classification"] = "Quarentena"
                        r["treatment"] = "quarantine"

                results.append(r)

        return results