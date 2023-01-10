{% docs fi_ar_4 %}
Accounts receivable: Line items
-------------------------------

This InfoSource shows the line items in Accounts Receivable Accounting from tables BSID (open items) and BSAD (cleared items) in the SAP ERP source system.

This InfoSource 0FI_AR_4 replaces the former InfoSource 0FI_AR_3. It provides an adapted extraction method (reads the tables directly rather than capturing the records during update). The communication structures of the former InfoSource 0FI_AR_3 and the new InfoSource 0FI_AR_4 are identical. As a result, the update rules for the data targets (ODS tables) can be kept. The migration from the former InfoSource, 0FI_AR_3, to the new one, 0FI_AR_4, is described in SAP note 410797Information published on SAP site.

**Source structure:** BSID (open items), BSAD (closed items)

**Extractor:** BWFID_GET_FIAR_ITEM

**Extract structure OLTP:** DTFIAR_3

Delta Update

This DataSource is delta-compatible.
{% enddocs %}
