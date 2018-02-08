# SimpleMerge
SimpleMerge builds and runs a `MERGE` statement to satisfy one specific (but very common) ETL design pattern: replace the data in a table with those from another. Depending on the number of columns involved, this can be an incredibly cumbersome statement, that can often be derived from only three parameters: the source, the target, and the key.

# Why
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

It's wordy, but that's because it's flexible. When that flexibility is not needed, updating a table from its source becomes a much simpler proposition:

```SQL
exec sp_SimpleMerge
	@target = 'MyDB.dbo.VendorList'
	,@source = 'SourceDB.dbo.VendorList'
	,@joinColumns = 'Vendor'
	;
```

# How
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
* ### @output
  dumps `MERGE` output to an audit table; must be a three-part name (`database.schema.table`) unless it is a temp table; 
  * leave `NULL` to suppress output clause
* ### @threshold
  only executes `MERGE` if rowcounts are within acceptable variance. will be ignored if **@target** has no rows.
  * use `'debug'` to print rowcounts and show constructed `MERGE` statement and creation script for output table
  * use `NULL` to ignore variance

