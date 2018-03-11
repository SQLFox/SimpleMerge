# SimpleMerge
SimpleMerge builds and runs a `MERGE` statement to satisfy one specific (but very common) ETL design pattern: replace the data in a table with those from another. Depending on the number of columns involved, building the `MERGE` can be an incredibly cumbersome and repetitive process. SimpleMerge boils it down and lets the table definitions do the driving.

Merging two tables becomes a procedure call with as few as three parameters: the source, the target, and the key.

# What else?
The use case for SimpleMerge is expected to be pretty narrow, but it does contain a couple of bells and whistles in addition to just building and executing the `MERGE`.
* ### lastUpdate
  SimpleMerge adds an extended property `lastUpdate` to the target. After the `MERGE` completes, that property is updated with `SYSDATETIME()`. This is intended to provide an easy way to know when a table was last updated from its source without having to go dig through the job history.
* ### @threshold
  The optional **@threshold** parameter aborts the `MERGE` if too many rows would be modified. This provides a bit of protection against an unexpectedly empty/malformed source table, hoping to prevent ETL errors from cascading into production issues.

# But what if...?
* ### ...I don't want some random code from the internet to change my data?
  Use the **@debug** parameter to show you the `MERGE` it *would* have run.
* ### ...I don't know what format the output will be in?
  The aforementioned **@debug** parameter will also build a `CREATE TABLE` script to accept the `MERGE` output.
* ### ...every column is in the key?
  That's fine; there just won't be a `WHEN MATCHED` clause.
* ### ...my key is lousy?
  Even if there are nullable columns in the key, SimpleMerge can handle it.
* ### No, seriously, my key is *that bad*.
  I get it; it's not your fault. Sometimes you get stuck with a source that has legitimate duplicate rows, even if you consider every column your key. The **@badKey** parameter will force every row to be unique (without changing the output) to allow the `MERGE`. Because order is not guaranteed, this may, in some circumstances, lead to updates of unchanged rows (i.e. two rows with an identical key could "swap places").

# How?
* ### @target
  must be a three-part name (`database.schema.table`) unless it is a temp table; may contain columns not present in **@source**
* ### @source
  must be a three-part name (`database.schema.table`) unless it is a temp table; every column in the source table **must** exist in **@target**
* ### @joinColumns
  is a comma separated list of columns to be used as the key (the columns in the `ON` clause of the `MERGE`)
* ### @delete
  controls the `WHEN NOT MATCHED BY SOURCE` action
  * use `'YES'` to delete records from **@target** *(default)*
  * use `'set <expression>'` to update **@target** (e.g. `update set isDeleted = 1`)
  * any other value omits the `WHEN NOT MATCHED BY SOURCE` clause
* ### @targetFilter
  uses a CTE to limit the effect of the `MERGE` on the target
  * use `'<expression>'` as you would in a where clause (e.g. `isDeleted = 0`)
* ### @badKey
  uses CTEs for both the target and source to add uniqueness to the key
* ### @output
  dumps `MERGE` output to an audit table; must be a three-part name (`database.schema.table`) unless it is a temp table 
  * leave `NULL` to suppress output clause
* ### @threshold
  only executes `MERGE` if percentage of rows changed is within acceptable variance; will be ignored if **@target** has no rows.
  * use `NULL` to ignore variance
* ### @debug
  shows constructed `MERGE` statement and creation script for output table; does not execute the `MERGE`

# Examples
When needing to update a table (such as below) from a source,

Vendor | Name | Addr | Addr2 | City | State | Zip | Phone
--- | --- | --- | --- | --- | --- | --- | ---
1 | Acme | 101 Main St | | Townville | TX | 77777 | (555) 555-1212

a `MERGE` statement would look like this:

```SQL
MERGE INTO MyDB.dbo.VendorList t
	USING SourceDB.dbo.VendorList s
		ON t.[Vendor] = s.[Vendor]
	WHEN MATCHED AND NOT EXISTS (
			SELECT s.[Name], s.[Addr], s.[Addr2], s.[City], s.[State], s.[Zip], s.[Phone]
			INTERSECT
			SELECT t.[Name], t.[Addr], t.[Addr2], t.[City], t.[State], t.[Zip], t.[Phone]
			)
		THEN UPDATE SET
			t.[Name] = s.[Name]
			,t.[Addr] = s.[Addr]
			,t.[Addr2] = s.[Addr2]
			,t.[City] = s.[City]
			,t.[State] = s.[State]
			,t.[Zip] = s.[Zip]
			,t.[Phone] = s.[Phone]
	WHEN NOT MATCHED BY TARGET
		THEN INSERT (
			[Vendor]
			,[Name]
			,[Addr]
			,[Addr2]
			,[City]
			,[State]
			,[Zip]
			,[Phone]
			)
		VALUES (
			s.[Vendor]
			,s.[Name]
			,s.[Addr]
			,s.[Addr2]
			,s.[City]
			,s.[State]
			,s.[Zip]
			,s.[Phone]
			)
	WHEN NOT MATCHED BY SOURCE
		THEN DELETE
	;
```
becomes:
```SQL
exec sp_SimpleMerge
	@target = 'MyDB.dbo.VendorList'
	,@source = 'SourceDB.dbo.VendorList'
	,@joinColumns = 'Vendor'
	;
```
