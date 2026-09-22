from data_generator.config import GeneratorConfig
from data_generator.fraud_scenarios import FraudScenarioInjector


def test_generator_config_defaults():
    config = GeneratorConfig()
    assert config.customers == 100000
    assert config.transactions == 5000000
    assert config.batch_size == 200000


def test_fraud_injection_is_reproducible():
    rows = [{"transaction_id": f"TXN-{i:08d}"} for i in range(1000)]
    injected_a = FraudScenarioInjector(prevalence=0.008, seed=42).inject(rows)
    injected_b = FraudScenarioInjector(prevalence=0.008, seed=42).inject(rows)
    assert injected_a == injected_b


def test_transaction_identity_not_reused():
    rows = [{"transaction_id": f"TXN-{i:08d}"} for i in range(10)]
    assert len(set(r["transaction_id"] for r in rows)) == len(rows)
