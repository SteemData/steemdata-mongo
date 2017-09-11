## SteemData
The goal of the SteemData project is to make data from the
STEEM blockchain more accessible to developers, researchers and 3rd party services.

SteemData is currently powered by MongoDB, the most used nosql, and [5th](http://db-engines.com/en/ranking) most popular database solution in the world.
MongoDB comes with a powerful query language, and it allows us to easily accommodate blockchain changes thanks to schema flexibility.

With SteemData, you can query for any kind of information living on the STEEM blockchain, such as Accounts, Posts, Transactions, Blocks or any kind of Operations.


### Getting Started
I highly recommend [RoboMongo](https://robomongo.org/) as a GUI utility for exploring SteemData.

**Connection Details**
>Host: mongo1.steemdata.com   
Port: 27017   
Database: SteemData   
Username: steemit   
Password: steemit  

![](https://i.gyazo.com/7717985009640f28083efa5aaca7a72d.png)


## Deployment
Create a read-only steemit user from createUser.mongo
from mongodb shell when mongo first starts.