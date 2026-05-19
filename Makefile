E2E_RUNNER := ./run_e2e_tests.sh

.PHONY: ci test-e2e

ci: test-e2e

test-e2e:
	$(E2E_RUNNER) all
