"""Testes automatizados da classe DataQuality.

Executar no Databricks:
    dbutils.notebook.run('tests/test_data_quality', 0)

Ou via pytest com SparkSession disponível:
    pytest tests/test_data_quality.py -v
"""
import importlib.util
import sys
import os

# Carrega a classe DataQuality do módulo externo
_DQ_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'utils', 'data_quality.py'
)
_RULES_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'dq_rules', 'bronze.json'
)

spec = importlib.util.spec_from_file_location('data_quality', _DQ_PATH)
_dq_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_dq_mod)

# Injeta a SparkSession do Databricks no escopo do módulo
if 'spark' in dir():
    _dq_mod.spark = spark
else:
    from pyspark.sql import SparkSession
    _dq_mod.spark = SparkSession.builder.getOrCreate()

DataQuality = _dq_mod.DataQuality

# =========================================================
# Configuração: cria tabelas temporárias para testes
# =========================================================

CATALOG = 'dataquality_challenge'
SCHEMA = 'bronze'

# Cria schema temporário para testes (não interfere no pipeline)
_dq_mod.spark.sql(f'CREATE SCHEMA IF NOT EXISTS {CATALOG}.test_dq')

_dq_mod.spark.sql(f'''
CREATE OR REPLACE TABLE {CATALOG}.test_dq.sample_dim AS
SELECT * FROM VALUES
  ('S001', 'PR', 'T001', 'V001'),
  ('S002', 'SP', 'T002', 'V002'),
  ('S003', NULL, 'T003', 'V003'),
  ('S004', 'MG', NULL, 'V004'),
  ('S001', 'PR', 'T001', 'V001')
AS (store_id, state, territory_id, seller_id)
''')

_dq_mod.spark.sql(f'''
CREATE OR REPLACE TABLE {CATALOG}.test_dq.sample_fact AS
SELECT * FROM VALUES
  ('2025-01', 'S001', 'EAN001', 100.0, 50),
  ('2025-01', 'S001', 'EAN001', 100.0, 50),
  ('2025-01', 'S002', 'EAN002', -50.0, 10),
  ('2025-01', 'S003', 'EAN003', 200.0, 30),
  ('2025-01', 'S999', 'EAN999', 300.0, 40)
AS (week, store_id, ean, sales_value_brl, sold_volume)
''')

# =========================================================
# Testes dos métodos de checagem
# =========================================================

def test_check_nulls():
    """check_nulls deve contar valores nulos corretamente."""
    dq = DataQuality(CATALOG, 'test_dq', 'sample_dim', _RULES_PATH)
    dq.table = 'sample_dim'
    result = dq.check_nulls('state')
    assert result['fail_count'] == 1
    assert result['percent'] == 20.0
    assert result['check'] == 'Nulls'
    print('PASS: test_check_nulls')

def test_check_duplicates():
    """check_duplicates deve contar chaves duplicadas corretamente."""
    dq = DataQuality(CATALOG, 'test_dq', 'sample_dim', _RULES_PATH)
    dq.table = 'sample_dim'
    result = dq.check_duplicates(column='store_id')
    assert result['check'] == 'Duplicates'
    assert result['fail_count'] == 1
    print('PASS: test_check_duplicates')

def test_check_empty_strings():
    """check_empty_strings deve contar strings vazias corretamente."""
    dq = DataQuality(CATALOG, 'test_dq', 'sample_dim', _RULES_PATH)
    dq.table = 'sample_dim'
    result = dq.check_empty_strings('state')
    assert result['check'] == 'Empty Strings'
    assert result['percent'] == 0.0
    print('PASS: test_check_empty_strings')

def test_check_negatives():
    """check_negatives deve contar valores negativos corretamente."""
    dq = DataQuality(CATALOG, 'test_dq', 'sample_fact', _RULES_PATH)
    dq.table = 'sample_fact'
    result = dq.check_negatives('sales_value_brl')
    assert result['fail_count'] == 1
    assert result['percent'] == 20.0
    assert result['check'] == 'Negatives'
    print('PASS: test_check_negatives')

def test_check_domain():
    """check_domain deve identificar valores fora do domínio."""
    dq = DataQuality(CATALOG, 'test_dq', 'sample_dim', _RULES_PATH)
    dq.table = 'sample_dim'
    result = dq.check_domain('state', ['PR', 'SP', 'MG'])
    assert result['check'] == 'Unexpected Domain'
    # NULL não conta como inesperado; apenas valores não-NULL fora do domínio
    print('PASS: test_check_domain')

def test_check_date_range():
    """check_date_range deve identificar intervalos inválidos."""
    dq = DataQuality(CATALOG, 'test_dq', 'sample_fact', _RULES_PATH)
    dq.table = 'sample_fact'
    # Criar tabela com datas para testar
    _dq_mod.spark.sql(f'''
    CREATE OR REPLACE TABLE {CATALOG}.test_dq.sample_dates AS
    SELECT * FROM VALUES
      ('2025-01-01', '2025-01-31'),
      ('2025-02-01', '2025-01-15'),
      ('2025-03-01', '2025-03-31')
    AS (start_date, end_date)
    ''')
    dq.table = 'sample_dates'
    result = dq.check_date_range('start_date', 'end_date')
    assert result['fail_count'] == 1
    assert result['check'] == 'Invalid Date Range'
    print('PASS: test_check_date_range')

# =========================================================
# Testes do validate() com regras do bronze.json
# =========================================================

def test_validate_bronze_dim_loja():
    """validate() deve executar todas as regras para dim_loja na bronze."""
    dq = DataQuality(CATALOG, SCHEMA, 'dim_loja', _RULES_PATH)
    results = dq.validate()
    assert len(results) > 0
    assert all('rule_id' in r for r in results)
    assert all('classification' in r for r in results)
    assert all('severity' in r for r in results)
    assert all('fail_count' in r for r in results)
    assert all('treatment' in r for r in results)
    # dim_loja deve ter pelo menos 2 regras FAIL (territory_id, seller_id)
    failed = [r for r in results if r['status'] == 'FAIL']
    assert len(failed) >= 2
    print(f'PASS: test_validate_bronze_dim_loja ({len(results)} regras, {len(failed)} FAIL)')

def test_validate_bronze_dim_produto():
    """validate() deve executar todas as regras para dim_produto na bronze."""
    dq = DataQuality(CATALOG, SCHEMA, 'dim_produto', _RULES_PATH)
    results = dq.validate()
    assert len(results) > 0
    # dim_produto deve ter pelo menos 1 regra FAIL (ean duplicates)
    failed = [r for r in results if r['status'] == 'FAIL']
    assert len(failed) >= 1
    print(f'PASS: test_validate_bronze_dim_produto ({len(results)} regras, {len(failed)} FAIL)')

def test_validate_classification_corrigido():
    """Regras FAIL com safe_auto_fix=true devem ser classificadas como 'Corrigido automaticamente'."""
    dq = DataQuality(CATALOG, SCHEMA, 'dim_loja', _RULES_PATH)
    results = dq.validate()
    corrigido = [r for r in results if r['classification'] == 'Corrigido automaticamente']
    assert len(corrigido) >= 2
    assert all(r['treatment'] == 'auto_fix' for r in corrigido)
    print(f'PASS: test_validate_classification_corrigido ({len(corrigido)} regras)')

def test_validate_classification_aprovado():
    """Regras PASS com percent=0 devem ser classificadas como 'Aprovado'."""
    dq = DataQuality(CATALOG, SCHEMA, 'dim_calendario', _RULES_PATH)
    results = dq.validate()
    aprovado = [r for r in results if r['classification'] == 'Aprovado']
    assert len(aprovado) >= 1
    assert all(r['percent'] == 0 for r in aprovado)
    print(f'PASS: test_validate_classification_aprovado ({len(aprovado)} regras)')

def test_silver_dq_zero_fail():
    """A validação DQ na silver deve ter 0 regras FAIL (evidência de qualidade)."""
    silver_tables = ['dim_calendario', 'dim_loja', 'dim_produto',
                     'fact_market_share_provider_a', 'fact_market_share_provider_b',
                     'coverage_provider', 'customer_territory_history']
    all_results = []
    for t in silver_tables:
        dq = DataQuality(CATALOG, 'silver', t, _RULES_PATH)
        all_results.extend(dq.validate())
    failed = [r for r in all_results if r['status'] == 'FAIL']
    assert len(failed) == 0, f'{len(failed)} regras FAIL na silver: {[r["rule_id"] for r in failed]}'
    print(f'PASS: test_silver_dq_zero_fail ({len(all_results)} regras, 0 FAIL)')

# =========================================================
# Runner — executar todos os testes
# =========================================================

def run_all_tests():
    """Executa todos os testes em sequência e imprime o resumo."""
    tests = [
        test_check_nulls,
        test_check_duplicates,
        test_check_empty_strings,
        test_check_negatives,
        test_check_domain,
        test_check_date_range,
        test_validate_bronze_dim_loja,
        test_validate_bronze_dim_produto,
        test_validate_classification_corrigido,
        test_validate_classification_aprovado,
        test_silver_dq_zero_fail,
    ]
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f'FAIL: {test.__name__} — {e}')
            failed += 1
    print(f'\n=== RESUMO DOS TESTES ===')
    print(f'  Pass: {passed}')
    print(f'  Fail: {failed}')
    print(f'  Total: {passed + failed}')
    return failed == 0

if __name__ == '__main__':
    run_all_tests()