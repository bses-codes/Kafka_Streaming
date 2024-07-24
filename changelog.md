### v1.2.0 (Latest)
- Separated fields for adding credentials of producer database and consumer database.

<hr>

### v1.1.1 
- Added scripts for encryption, decryption.

<hr>

### v1.1.0

- Producer retrieves data from the database and encrypts the account number before streaming it.
- Consumer decrypts the encrypted account number and sends it to database.

<hr>

### v1.0.0

- Producer retrieves data from MySQL database and sends the data through Kafka Topic and Consumer consumes the data storing it in MySQL Database.



