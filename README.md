# Statelam MapReduce

This is about creating a Python object that 'exists on the cloud'.
Meaning: its attributes are stored online (REDIS initially).

The goal is to create AWS lambdas that could use this object to have a state
shared among instances of the same lambda.


**Main objective**: provide lambda an object that allows instances of the same function
to share state through a disaggregate service. But trying to make this disaggregation
invisible so it seams the programmer uses a python object that is shared among
the instances of the lambda.

To test the thing: implement a map-reduce structure over lambda. Specifically
implementing the basic wordcount example.

Needed to run:
* AWS Lambda
* SQS
* Boto3 configured

Configure your services at config.py
