## **Introduction**

This is an example storage system (a blockchain) for a cryptocurrency. It is modeled after [Bitcoin’s storage system](https://en.bitcoin.it/wiki/Bitcoin_Core_0.11_(ch_2):_Data_Storage), though heavily simplified.

### **Components**

Chain consists of several main components. Here’s a quick overview:

1. **BlockChain**
    - The main type of this project, **BlockChain** is a blockchain that stores and validates **Blocks**. It manages and updates its main chain based on the Blocks it receives, and deals with a fork situation, should one occurs.
2. **BlockInfoDatabase**
    - The **BlockInfoDatabase** is a wrapper for a [LevelDB](https://en.wikipedia.org/wiki/LevelDB), storing information about each Block it receives in the form of a **BlockRecord**. In addition, each BlockRecord contains storage information for an **UndoBlock**, which provides additional information to revert a Block, should a fork occur.
3. **ChainWriter**
    - The **ChainWriter** takes care of all I/O for the BlockChain. It writes and reads Blocks and UndoBlocks to Disk.
4. **CoinDatabase**
    - **CoinDatabase** stores all UTXO information. It contains a cache of **Coins** and a LevelDB of **CoinRecords**. A Coin is a wrapper for a UTXO, and a CoinRecord is a record of all the UTXO created by a Transaction. Validation needs to be as quick as possible, which is why the CoinDatabase contains an in-memory cache in addition to a persistent database. Eventually the cache becomes too large, at which point the CoinDatabase must flush its cache to its database.# blockchain_storage_system
