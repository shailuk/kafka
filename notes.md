1) Error - Missing Library - 
Root cause: pyodbc on macOS requires ODBC driver libraries that were missing from your system.

Fixed by installing dependencies:

Installed unixodbc via Homebrew (provides ODBC framework)
Reinstalled pyodbc package with --force-reinstall to link against the newly installed ODBC libraries
Installed Microsoft ODBC driver for SQL Server (msodbcsql17 and mssql-tools) via Homebrew
Result: The pyodbc import error was resolved. Your script now has all required ODBC dependencies to connect to SQL Server.

2) Make sure to install the following packages for the code to work
pip install certifi
pip install httpx
pip install authlib
pip install cachetools
pip install attrs
pip install fastavro
(Check out requirements.txt file)

Move all the libraries into requirement.txt file
pip freeze > requirements.txt

Install all the libraries in requirements.txt file 
pip install -r requirements.txt

3) How to run the code?
I used SQL Server as my source data. It was hosted on AWS. 
Insert some sample data into the table in SQL Server. 
I also a separate table to track the last fetched date

Run consumer first - 
python3 kafka_consumer.py

Run producer - 
python3 kafka_consumer.py


