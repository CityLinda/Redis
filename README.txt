Usage:
	Google BigQuery API to pull data from Google Cloud.
	Python pipeline to process BigQuery large result set in JSON format.
	Processes JSON in python dictionary and inserts into Reid as HASH data type using
	HSET.

	In order to improve performance, instead of insert one key->pair at the time, we use the "pipe"
	API provided by Redis. 

	All commands being buffered into the pipeline and executed in batch fashion
	pipelines can also ensure the buffered commands are executed atomically as a group. 


	EXEC_POINT is a counter to track the buffer size when applying buffered execution. The optimized 
	size depends on individual environment.

Prerequisite:
	Install Python Redis client on Python 2.7.9 or higher.

		$sudo pip install redis

	Install Google BigQuery client library:

		$sudo pip install --upgrade google-api-python-client

	Bigquery Authentication:

     		Locate your project and service account, download the json file assoicated with the service acount,
		place the json file on the same directory as the script which is accessing BigQuery. 
                If you’re running your application elsewhere, you should download a service account JSON keyfile and point to it using an environment variable:

                $export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
