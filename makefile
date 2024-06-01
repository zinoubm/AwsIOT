setup:
	unzip data/IOT-temp.csv.zip -d data
	export AWS_DEFAULT_REGION=us-east-2

simulate:
	python data/iot_simulation.py