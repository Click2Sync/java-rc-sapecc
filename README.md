# java-rc-sapecc
Java implementation of the reverse connector API of Click2Sync for SAP ECC 6 via BAPI with SAP Java Connector JCO3

## Running

1. Load in eclipse as eclipse project
2. Java 1.8 or later
3. Missing libraries (check .classpath file to understand which libraries to include)
4. Missing sapjco3.dll sapjco3.jar, sapjco3.pdb, etc. (you need to download from your SAP Account)

## Notices

- More reference on table mapping, bapi procedures, or ABAP procedures execution:
    - https://www.sapdatasheet.org/

- This is the example of a SAPB1 reverse connector implementation for:
    - Products readonly
    - Orders read/write

- But can be implemented also for products read/write if needed, just by calling the right SAP JCO ABAP methods

## C2S Reverse Connector Protocol & API Reference

https://www.click2sync.com/developers/reverse.html