.PHONY: develop
develop:
	python3 setup.py develop

.PHONY: test
test:
	cd tests && python3 -m unittest -bv