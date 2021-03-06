USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ================================================================================================
-- Author:		Scott Minar
-- Create date:	2015-07-22
-- Description:	Executes a standardized merge statement for two identical tables
--				@target, @source, and @output must use three-part names (database.schema.table)
--				@joinColumns is a comma separated list of columns to be joined on (up to 100)
--				@delete controls the "when not matched by source" action
--					* use 'YES' to delete records from @target (currently the default action)
--					* use 'set <expression>' to update @target (e.g. update set isDeleted = 1)
--					* any other value omits the "when not matched by source" clause
--				@targetFilter uses a CTE to limit the effect of the merge on the target
--					* use '<expression>' as you would in a where clause (e.g. isDeleted = 0)
--				@badKey uses CTEs for both target and source to add uniqueness to the key
--				@output dumps merge output to an audit table; leave NULL to suppress output clause
--				@threshold only executes merge if percentage of rows changed is within acceptable
--						variance. will be ignored if @target has no rows.
--					* use NULL to ignore variance
--				@debug shows constructed merge statement and creation script for output table
-- Change log:	2015-08-11 - SM - suppressed "when matched" clause when all columns are part of key
--				2015-09-29 - SM - built toolbox.mergeIf as wrapper for calling simplemerge only if
--					change threshold percentage is not exceeded
--				2016-03-11 - SM - Added lastUpdate extended property
--				2016-04-04 - SM - added column list to output clause to allow output tables to 
--					contain other columns; fixed output clause handlding when all columns are part
--					of key; persisted sys.columns data to eliminate redundant queries; copied split
--					function code to eliminate dependance on other UDF
--				2016-04-29 - SM - Added actual variance to threshold error
--				2016-07-05 - SM - Added logic to bypass threshold if target table is empty
--				2016-10-04 - SM - Combined simplemerge function and mergeIf proc into one proc.
--					Added additional parameter validation. Eliminated any reliance on current data-
--					base to allow proc to be housed in the master database and used in any other.
--				2016-10-28 - SM - Added NULL default value to @output and @threshold
--					Modified @source validation to prevent "Database name 'tempdb' ignored..."
--				2017-02-15 - SM - Changed debug messages from text to grid xml to allow output of
--					complete statements. Added additional parameter validation. Changed column
--					lists inside merge statement to source table to allow @source to be a subset of
--					columns from @target. Added @delete parameter. Added output table creation
--					script to debug output.
--				2017-07-26 - SM - Added @targetFilter parameter. Increased size of @variance to
--					accomodate significant row count discrepancies. Added default table name to 
--					output table creation script.
--				2018-02-07 - SM - Added rowcounts to debug grid output. Formatted [SimpleMerge] to
--					preserve XML special characters.
--				2018-02-21 - SM - Added dedicated @debug parameter. Replaced rowcount threshold
--					with actual percentage of rows modified by merge.
--				2018-03-09 - SM - Added handling for nullable key columns; Added more useful error
--					when SHOWPLAN breaks dm_exec_describe_first_result_set
--				2018-03-11 - SM - Added @badKey parameter; Added parsename() to @joinColumns
--					handling
-- /*==============================================================================================
ALTER proc [dbo].[sp_SimpleMerge] (
	@target varchar(256)
	,@source varchar(256)
	,@joinColumns varchar(max)
	,@delete varchar(max) = 'YES'
	,@targetFilter varchar(max) = null
	,@badKey bit = 0
	,@output varchar(256) = null
	,@threshold varchar(6) = null
	,@debug bit = 0
	) as begin
--*/
set nocount on;
/*
declare @threshold varchar(6) = '15%'
declare @target varchar(256) = 'Exports.payroll.Punch'
declare @source varchar(256) = 'Exports.payroll.Punch'
declare @joinColumns varchar(max) = 'Employee,Date,Department,Job,PayCategory,Start'
declare @delete varchar(max) = 'YES'
declare @targetFilter varchar(max) = '[Date] >= ''2017-12-01'''
declare @badKey bit = 1
declare @output varchar(256) = null
declare @debug bit = 1
if object_id('tempdb..#columnList') is not null drop table #columnList;
--*/
/* ================================================================================================
parameter validation
================================================================================================ */
declare @error varchar(256);
declare @sql nvarchar(max);
declare @matchedSQL varchar(max) = ''
declare @variance decimal(7,1);
declare @tCount bigint;
declare @sCount bigint;
declare @mergeCount bigint;
if @threshold is not null and isnumeric(replace(@threshold,'%','')) = 0
	set @error = '
	Threshold: ' + isnull(@threshold,'') + ' is invalid.
	';
declare @verboseTable varchar(256) = case when @target like '#%' then 'tempdb..' else '' end + @target;
declare @tObject int = (select object_id(@verboseTable));
if @tObject is null
	set @error = '
	Target: ' + isnull(@target,'') + ' not found.
	';
declare @tDB varchar(256) = parsename(@verboseTable,3);
if @tDB is null
	set @error = '
	Target: ' + isnull(@target,'') + ' is invalid. Database name is required.
	';
declare @tSchema varchar(256) = parsename(@verboseTable,2);
declare @tTable varchar(256) = parsename(@verboseTable,1);
set @verboseTable = case when @source like '#%' then 'tempdb..' else '' end + @source;
declare @sObject int = (select object_id(@verboseTable));
if @sObject is null
	set @error = '
	Source: ' + isnull(@source,'') + ' not found.
	';
declare @sDB varchar(256) = parsename(@verboseTable,3);
if @sDB is null
	set @error = '
	Target: ' + isnull(@source,'') + ' is invalid. Database name is required.
	';
if @output is not null begin
	set @verboseTable = case when @output like '#%' then 'tempdb..' else '' end + @output;
	declare @oObject int = (select object_id(@verboseTable));
	if @oObject is null
		set @error = '
		Output: ' + isnull(@output,'') + ' not found.
		';
	end
if @error is not null throw 50000, @error, 1;
/* ================================================================================================
add extended property "lastUpdate" to target table if missing
================================================================================================ */
set @sql = N'
if not exists (
		select 1 from ' + @tDB + '.sys.extended_properties
			where major_id = ' + cast(@tObject as varchar(12)) + '
				and name = ''lastUpdate''
		) begin
	exec ' + @tDB + '.dbo.sp_addextendedproperty  @name  = N''lastUpdate'',  @value = ''new'', @level0type = N''SCHEMA'', @level0name = ''' + @tSchema + ''', @level1type = N''TABLE'', @level1name = ''' + @tTable + ''';
	end';
exec (@sql);
/* ================================================================================================
get row count for target
================================================================================================ */
if @threshold is not null or @debug = 1 begin
	set @sql = N'
		set @tCountOUT = (select count(*) from ' + @target + isnull(' where ' + @targetFilter,'') + ');
';	exec sp_executesql @sql, N'@tCountOUT bigint OUTPUT'
		,@tCountOUT = @tCount OUTPUT
		;
	end
/* ================================================================================================
build merge components
================================================================================================ */
		create table #columnList (
			name sysname null
			,sourceId int null
			,targetId int null
			,joinCol int null
			,system_type_name varchar(128) null
			,is_nullable bit null
			);
		set @sql = N'
		WITH E1(N) AS (		/*splitting code copied from Jeff Moden''s [DelimitedSplit8K]*/
			SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
			SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
			SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
			)
		,E2(N) AS (SELECT 1 FROM E1 a, E1 b)
		,cteTally(N) AS (SELECT TOP (ISNULL(DATALENGTH(@pString),0)) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E2)
		,cteStart(N1) AS (SELECT 1 UNION ALL SELECT t.N+1 FROM cteTally t WHERE SUBSTRING(@pString,t.N,1) = '','')
		,cteLen(N1,L1) AS (SELECT s.N1, ISNULL(NULLIF(CHARINDEX('','',@pString,s.N1),0)-s.N1,8000) FROM cteStart s)
		,split as (SELECT ItemNumber = ROW_NUMBER() OVER(ORDER BY l.N1),Item = SUBSTRING(@pString, l.N1, l.L1) FROM cteLen l)
		select
			name = isnull(sc.name,tc.name)
			,sourceId = sc.column_id
			,targetId = tc.column_ordinal
			,joinCol = s.ItemNumber
			,tc.system_type_name
			,tc.is_nullable
			from (
				select
					name
					,column_id
					from ' + quotename(@sDB) + '.' + 'sys.columns
					where object_id = ' + cast(@sObject as varchar(12)) + '
				) sc
			full outer join sys.dm_exec_describe_first_result_set(''select * from ' + @target + ''',null,0) tc
				on sc.name = tc.name
			full outer join split s
				on parsename(ltrim(rtrim(s.Item)),1) = sc.name
				or parsename(ltrim(rtrim(s.Item)),1) = tc.name
';		insert into #columnList
			exec sp_executesql @sql, N'@pString varchar(8000)', @pString = @joinColumns;
		if exists (select 1 from #columnList where [name] is null and targetId = 0)
			throw 50000, 'sp_describe_first_result_set cannot be invoked when SET STATISTICS XML or SET STATISTICS PROFILE is on.', 1;
		if exists (select 1 from #columnList where joinCol is not null and sourceId is null)
			throw 50000, 'Specified join column missing from source.', 1;
		if exists (select 1 from #columnList where joinCol is not null and targetId is null)
			throw 50000, 'Specified join column missing from target.', 1;
		if exists (select 1 from #columnList where sourceId is not null and targetId is null)
			throw 50000, 'Specified source column missing from target.', 1;
/* ================================================================================================
prefix
================================================================================================ */
declare @mergePrefix varchar(max) = case
	when @targetFilter is not null or @badKey = 1 then 
		'with [target] as (
	select *' + case
			when @badKey = 1 then '
		,_SimpleMerge_rn = row_number() over(
			partition by 
				' + stuff((
				select '
				,' + quotename([name])
					from #columnList
						where joinCol is not null
						order by joinCol
						for xml path(''), type
						).value('.','varchar(max)'),1,7,'') + '
			order by %%physloc%%)'
			else '' end + '
		from ' + @target + case
			when @targetFilter is not null then '
		where ' + @targetFilter
			else '' end + '
	)' + case
			when @badKey = 1 then ', [source] as (
	select *
		,_SimpleMerge_rn = row_number() over(
			partition by 
				' + stuff((
				select '
				,' + quotename([name])
					from #columnList
						where joinCol is not null
						order by joinCol
						for xml path(''), type
						).value('.','varchar(max)'),1,7,'') + '
			order by %%physloc%%)
		from ' + @source + '
	)'
			else '' end + '
merge into [target] t
	using ' + case
			when @badKey = 1 then '[source]'
			else @source end + ' s'
	else
		'merge into ' + @target + ' t
	using ' + @source + ' s'
	end
/* ================================================================================================
join condition
================================================================================================ */
declare @mergeJoin varchar(max) = '
		on ' + stuff((
			select
				'
		and (t.' + quotename(name) + ' = s.' + quotename(name) + case 
					when is_nullable = 0 then ')' else '
			or (t.' + quotename(name) + ' is null
				and s.' + quotename(name) + ' is null
				)
			)'		end
				from #columnList
				where joinCol is not null
				order by joinCol
				for xml path(''), type
			).value('.','varchar(max)'),1,8,'')
	+ case when @badKey = 1 then '
		and t._SimpleMerge_rn = s._SimpleMerge_rn'
		else '' end
/* ================================================================================================
when matched and not exists
================================================================================================ */
		if exists (select 1 from #columnList where joinCol is null and sourceId is not null)
			set @matchedSQL = '
	when matched and not exists (
			select ' + stuff((
				select
					', s.' + quotename(name)
					from #columnList
					where joinCol is null
						and sourceId is not null
					order by sourceId
					for xml path(''), type
				).value('.','varchar(max)'),1,2,'') + '
			intersect
			select ' + stuff((
				select
					', t.' + quotename(name)
					from #columnList
					where joinCol is null
						and sourceId is not null
					order by sourceId
					for xml path(''), type
				).value('.','varchar(max)'),1,2,'') + '
			)
		then update set
			' + stuff((
				select
					'
			,t.' + quotename(name) + ' = s.' + quotename(name)
					from #columnList
					where joinCol is null
						and sourceId is not null
					order by sourceId
					for xml path(''), type
				).value('.','varchar(max)'),1,6,'')
		;
/* ================================================================================================
assemble merge
================================================================================================ */
set @sql = @mergePrefix 
	+ @mergeJoin
	+ @matchedSQL + '
	when not matched by target
		then insert (
			' + stuff((
				select
					'
			,' + quotename(name)
					from #columnList
					where sourceId is not null
					order by sourceId
					for xml path(''), type
				).value('.','varchar(max)'),1,6,'') + '
			)
		values (
			' + stuff((
				select
					'
			,s.' + quotename(name)
					from #columnList
					where sourceId is not null
					order by sourceId
					for xml path(''), type
				).value('.','varchar(max)'),1,6,'') + '
			)
	' + case
		when @delete = 'YES' then
	'when not matched by source
		then delete
	'	when @delete like 'set %' then
	'when not matched by source
		then update ' + @delete + '
	'	else '' end
/* ================================================================================================
output clause
================================================================================================ */
	+ case when @output is not null then 'output
		sysdatetime()
		,$action
		' + stuff((
			select
				'
		,isnull(deleted.' + name + ',inserted.' + name + ')'
				from #columnList
				where joinCol is not null
				order by joinCol
				for xml path(''), type
			).value('.','varchar(max)'),1,4,'') + '
		' + case len(@matchedSQL) when 0 then '' else stuff((
				select
					'
		,deleted.' + quotename(name)
					from #columnList
					where joinCol is null
					order by targetId
					for xml path(''), type
				).value('.','varchar(max)'),1,4,'') + '
		' + stuff((
				select
					'
		,inserted.' + quotename(name)
					from #columnList
					where joinCol is null
					order by targetId
					for xml path(''), type
				).value('.','varchar(max)'),1,4,'') end + '
		into ' + @output + ' (
			actionTime
			,[action]
		' + stuff((
			select
				'
			,' + quotename(name) + ''
				from #columnList
				where joinCol is not null
				order by joinCol
				for xml path(''), type
			).value('.','varchar(max)'),1,4,'') + '
		' + case len(@matchedSQL) when 0 then '' else stuff((
				select
					'
			,[d_' + name + ']'
					from #columnList
					where joinCol is null
					order by targetId
					for xml path(''), type
				).value('.','varchar(max)'),1,4,'') + '
		' + stuff((
				select
					'
			,[i_' + name + ']'
					from #columnList
					where joinCol is null
					order by targetId
					for xml path(''), type
				).value('.','varchar(max)'),1,4,'') end + '
			)' else '' end + '
	;'
			;					
/* ================================================================================================
what was this for?
================================================================================================ */
		if @sql like '@%' begin
			set @sql = '
		' + @sql + '
		'	raiserror(@sql, 16, 1);
			end
		else
			if @debug = 1
/* ================================================================================================
debug mode output
================================================================================================ */
				select debug, [SQL] from (values
					('SimpleMerge', (select '--
' + @sql + '
--' as [processing-instruction(query)] for xml path(''), type))
					,('Threshold',replace(@threshold,'%','') + '%')
					,(@target + ' rowcount',cast(@tCount as varchar(20)))
					,('output table', cast('create table ' + isnull(@output,@target + '_SimpleMergeOutput') + ' (
	actionTime datetime2() not null
	,[action] varchar(10)
	' + stuff((
			select
				'
	,' + quotename(name) + ' ' + system_type_name
				from #columnList
				where joinCol is not null
				order by joinCol
				for xml path(''), type
			).value('.','varchar(max)'),1,3,'') + '
	' + case len(@matchedSQL) when 0 then '' else stuff((
				select
					'
	,[d_' + name + '] ' + system_type_name
					from #columnList
					where joinCol is null
					order by targetId
					for xml path(''), type
				).value('.','varchar(max)'),1,3,'') + '
	' + stuff((
				select
			'
	,[i_' + name + '] ' + system_type_name
					from #columnList
					where joinCol is null
					order by targetId
					for xml path(''), type
				).value('.','varchar(max)'),1,3,'') end + '
	);' as xml))
					) x (debug, [SQL])

					;
			else begin
/* ================================================================================================
execute merge
================================================================================================ */
				begin transaction;
					exec (@sql);
					set @mergeCount = @@ROWCOUNT
					set @variance = @mergeCount * 1.0 / nullif(@tCount,0) * 100;
					if @variance <= cast(replace(@threshold,'%','') as int)
						or @threshold is null
						or @tCount = 0	/*disregard threshold if target is empty*/
							commit transaction;
						else begin
							rollback transaction;
							set @error = '
		Merge aborted: variance exceeded ' + replace(@threshold,'%','') + '%% (' + cast(@variance as varchar(20)) + '%%).
		'
							raiserror(@error, 16, 1);
							end
/* ================================================================================================
update extended property "lastUpdate"
================================================================================================ */
				declare @ep varchar(50) = convert(varchar(50),cast(sysdatetime() as datetime2(3)),121);
				set @sql = N'
				exec ' + @tDB + '.dbo.sp_updateextendedproperty  @name  = N''lastUpdate'',  @value = ''' + @ep + ''', @level0type = N''SCHEMA'', @level0name = ''' + @tSchema + ''', @level1type = N''TABLE'', @level1name = ''' + @tTable + ''';'
				exec (@sql);
				end

end
