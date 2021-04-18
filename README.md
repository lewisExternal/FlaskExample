Flask Example 
==================================

To host an endpoint for which given a date argument, will return a user defined report in he JSON format for some test data. 

Installation
============

```console
$ pip install --upgrade pip 
$ pip install flask 
$ pip install pandas 
```
This has been tested with the following versions.

* Flask==1.1.2
* numpy==1.20.1

The user will need to run the following to create the DB from CSV files.
```console
$ python3 db_lib.py
```

Example usage for the endpoint 
===============================

To run the endpoint locally, run the below to run the Flask app. 
```console
$ python3 main.py
```
An example http request such as the below:
 
 http://127.0.0.1:5000/api/v1/report?date=2019-09-29

Will return for example the following: 
```console
{"commissions":{"order_average":130932.35,"promotions":{"1":null,"2":null,"3":null,"4":null,"5":null},"total":8248738.27},"customers":5,"discount_rate_avg":0.18,"items":1544,"order_total_avg":13202472.69,"total_discount_amount":12999485.95}
```
Testing
===============================

More specific unit tests need to be added. 