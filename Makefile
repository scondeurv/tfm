E2E_RUNNER := ./run_e2e_tests.sh

.PHONY: ci test-e2e test-correctness

ci: test-e2e test-correctness

test-e2e:
	$(E2E_RUNNER) all

# Oracle + property-based correctness suite (needs tests/.venv-test).
test-correctness:
	$(E2E_RUNNER) correctness
