import snowflake.connector
import pandas as pd

# Establish a connection to Snowflake
conn = snowflake.connector.connect(
    user='your_username',
    password='your_password',
    account='your_account'
)

# Create a cursor object
cur = conn.cursor()

# Iterate through the dataframe and insert each row
for index, row in df.iterrows():
    cur.execute(
        """
        INSERT INTO MYDB.PUBLIC.EMPDATA (ID, FIRST_NAME, LAST_NAME, EMAIL, GENDER, IP_ADDRESS)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (row['ID'], row['FIRST_NAME'], row['LAST_NAME'], row['EMAIL'], row['GENDER'], row['IP_ADDRESS'])
    )

# Close the cursor and connection
cur.close()
conn.close()
