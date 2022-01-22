set nocount on;

if object_id('tempdb..##LMH_AvgTurnDays_Final12249') is not null
drop Table [##LMH_AvgTurnDays_Final12249]

if object_id('tempdb..##lmhus12249]') is not null
drop Table  [##lmhunitstatusseq12249];

if OBJECT_ID('tempdb..##lmhunitstatusseq12249') is not null
	drop table [##lmhunitstatusseq12249]

CREATE TABLE [##LMH_AvgTurnDays_Final12249] (
	[HMY] NUMERIC(18, 0) IDENTITY(1, 1)
	,[downDate] date NULL
	,[downDate2] date NULL
	,[hunit] NUMERIC(18, 0) NULL
	,[uhmy] NUMERIC(18, 0) NULL
	,[hprop] NUMERIC(18, 0) NULL
	,countNotDown INT NULL
	,Region VARCHAR(100) NULL
	,District VARCHAR(100) NULL
	,Unit_Code VARCHAR(8) NULL
	,Unit_Address VARCHAR(512) NULL
	,BU VARCHAR(8) NULL
	,Property_Name VARCHAR(256) NULL CONSTRAINT [PK_##LMH_AvgTurnDays_Final12249] PRIMARY KEY NONCLUSTERED ([HMY] ASC) WITH (
		PAD_INDEX = OFF
		,STATISTICS_NORECOMPUTE = OFF
		,IGNORE_DUP_KEY = OFF
		,ALLOW_ROW_LOCKS = ON
		,ALLOW_PAGE_LOCKS = ON
		) ON [PRIMARY]
	) ON [PRIMARY];

CREATE INDEX ##IX_1 ON [##LMH_AvgTurnDays_Final12249](hunit,downDate, downDate2);

create table [##lmhunitstatusseq12249] (
	[Id] numeric(18, 0) IDENTITY(1, 1)
	,[RowID] int null
	,[hmy] numeric(18, 0) null
	,[hunit] numeric(18, 0) null
	,[dtstart] date null
	,dtend date null
	,[Type] varchar(100) null
	,[denserank] int null
	,[downCount] int null constraint [PK_##lmhunitstatusseq12249] primary key nonclustered ([Id] asc) with (
		PAD_INDEX = off
		,STATISTICS_NORECOMPUTE = off
		,IGNORE_DUP_KEY = off
		,ALLOW_ROW_LOCKS = on
		,ALLOW_PAGE_LOCKS = on
		) on [PRIMARY]
	) on [PRIMARY]

Declare @i int = 0, @month date = '01/01/2021';


declare @id int = 0
	,@d int = 0
	,@RowID int;



while (@i < 12)
begin
/*
* 03/10/1988 earliest date in unit status table
* SELECT CAST(MIN(DTSTART) AS DATE) FROM UNIT_STATUS
*/
;with C2
as (
	select *
		/* This sum() ... desc rows unbounded preceding ... section creates the number ordering RowID */
		/* which counts the change in status' between Occupied No Notice */
		,sum(case
				when Type = 'Occupied'
					then 1
				else 0
				end) over (
			order by hunit
				,dtstart desc
				,HMY desc rows unbounded preceding
			) as 'change'
	from (
		select row_number() over (
				order by MR.HUNIT
				/* do I want to sort by dtstart DESC too? Probably want to clean up the unit_status table */
				/* so that the ordering of the records match the date structure */
					,MR.DTSTART DESC
					,mr.HMY desc
				) rownum
			,mr.hmy
			,mr.hunit
			,mr.dtStart
			,mr.dtend
			,ltrim(replace(replace(replace(replace(replace(mr.sstatus, 'Vacant Rented', ''), 'Vacant Unrented', ''), 'Occupied No Notice', 'Occupied'), 'Admin', 'Down'), 'Model','Down')) Type
		from unit_status mr with (nolock)
		inner join unit u with (nolock) on u.hmy = mr.hUnit
		inner join property p with (nolock) on p.hmy = u.hproperty
		where 1=1
			and mr.sstatus in ('Admin', 'Down','Model')
			and (
				mr.dtstart <= EOMONTH(@month)
				and ISNULL(mr.dtend, EOMONTH(@month)) <= EOMONTH(@month)
			)
			   and p.hmy = 792 /* #Condition02# #Condition03# #Condition04#*/
		) c
	)
insert into [##lmhunitstatusseq12249] (
	RowID
	,[hmy]
	,[hunit]
	,[dtstart]
	,dtend
	,[Type]
	)
select RowID
	,[hmy]
	,[hunit]
	,[dtstart]
	,dtend
	,[Type]
from (
	/* This row_number() section creates the number ordering RowID which counts the change in status' between Occupied No Notice */
	select row_number() over (
			partition by change order by HUNIT
				,dtstart DESC
				,HMY desc
			) as RowID
		,*
	from C2
	) x
order by hUnit
	,dtstart desc
	,HMY desc;

/* had to use a cursor here because dense_rank() wasn't working apparently it's not deterministic
meaning the values returned won't always be the same even if all other things equal.
I don't really get it but:
https://dba.stackexchange.com/questions/77639/are-rank-and-dense-rank-deterministic-or-non-deterministic */


declare mycursor cursor FAST_FORWARD
for
select Id
	,RowID
from [##lmhunitstatusseq12249]
order by Id

open mycursor

fetch next
from mycursor
into @id
	,@RowID

while @@FETCH_STATUS = 0
begin
	if @RowID = 1
		set @d += 1;

	update l
	set l.denserank = @d
	from [##lmhunitstatusseq12249] l
	where l.Id = @id;

	fetch next
	from mycursor
	into @id
		,@RowID
end

close mycursor

deallocate mycursor;

with down
as (
	select *
	from (
		select row_number() over (
				partition by l.hunit
				,l.denserank order by l.hunit
					,l.dtstart
				) rownum
			,*
		from  [##lmhunitstatusseq12249] l
		where Type = 'Down'
		) l
	where rownum = 1
	)
INSERT INTO [##LMH_AvgTurnDays_Final12249] (
	BU
	,Unit_Code
	,Unit_Address
	,Property_Name
	,downDate
	,downDate2
	,hunit
	,uhmy
	,Region
	,District
	)
select RTRIM(p.scode) BU
	,rtrim(u.scode) Unit_Code
	,rtrim(isnull(a.saddr1, '')) + ' ' + rtrim(isnull(a.saddr2, '')) Unit_Address
	,rtrim(p.saddr1) Property_Name
	,@month downdate
	,@month downDate2
	,u.hmy hunit
	,u.hmy uhmy
	,rtrim([at].SUBGROUP25) District
	,rtrim([at].SUBGROUP27) Region
from down
join unit u on u.hmy = down.hunit
join property p on p.hmy = u.HPROPERTY
join attributes [at] on [at].HPROP = p.HMY
left join addr a on a.HPOINTER = u.HMY
	and a.itype = 4

set @month = dateadd(month, 1, @month);

set @i += 1;


end /*while */

declare @s nvarchar(max)
	,@cols nvarchar(max);

with PVTDATA
as (
	select COLS
	from (
		select FORMAT(downDate, 'M/yyyy') as COLS
			,downDate
		from [##LMH_AvgTurnDays_Final12249]
		where ISNULL(downDate2, @month) <= @month
		) X
	group by COLS
	)
select @cols = STUFF((
			select ',' + QUOTENAME(COLS) COLS
			from PVTDATA pvt
			order by RIGHT(COLS, 4)
				,CAST(case
						when LEFT(COLS, 2) like '%/%'
							then '0' + LEFT(COLS, 1)
						else LEFT(COLS, 2)
						end as int)
			for xml PATH('')
				,TYPE
			).value('.', 'NVARCHAR(MAX)'), 1, 0, '')
from PVTDATA pvt2;

set @s = '
;with PIVOT_DET
as (
	select Property_Name
		,DISTRICT
		,REGION
		,HPROP
		,RTRIM(BU) as BU '
		+ @cols + '
	from (
		select COUNT(HUNIT) MYCOUNT
			,Property_Name
			,BU
			,DISTRICT
			,REGION
			,HPROP
			,COLS
		from (
			select Property_Name
				,BU
				,DISTRICT
				,REGION
				,HPROP
				,HUNIT
				,FORMAT(COALESCE(downDate, ' + quotename(EOMONTH(@month), '''') + '), ''M/yyyy'') COLS
			from [##LMH_AvgTurnDays_Final12249]
			group by Property_Name
				,BU
				,DISTRICT
				,REGION
				,HPROP
				,HUNIT
				,FORMAT(COALESCE(downDate, ' + quotename(EOMONTH(@month), '''') + '), ''M/yyyy'')
			) X
		group by Property_Name
			,BU
			,DISTRICT
			,REGION
			,HPROP
			,COLS
		) as SourceTable
	PIVOT(SUM(MYCOUNT) for COLS in (' + SUBSTRING(@cols, 2, LEN(@cols)) + ')) as PivotTable
	)
select *
from PIVOT_DET; ';


exec sp_Executesql @s;
