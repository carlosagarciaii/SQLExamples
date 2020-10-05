use WGS84

/*

This script is meant to pull the best possible candidate Street Segment ID from the GIS Set.

Version:	1.4
Author:		Carlos A Garcia II
Date:		2016 11 22

-- */

set nocount ON
GO



begin try
		drop database GISRoadIDResults
end try
begin catch
end catch

GO

CREATE database GISRoadIDResults

GO



begin try
		drop table WGS84.dbo.FoundMatches
end try
begin catch
end catch


begin try
	drop table WGS84.dbo.FoundMatchesErrors
end try
begin catch
end catch

begin try
	drop table #tAddressParts
end try
begin catch
end catch

begin try
	drop table #tRoadParts
end try
begin catch
end catch


create table GISRoadIDResults.dbo.FoundMatches(
								GISAddressPointID nvarchar(max),
								GISRoadID int,
								GlobalID nvarchar(max)
								)

create table GISRoadIDResults.dbo.FoundMatchesErrors(
										GlobalID nvarchar(Max),
										GISAddressPointID nvarchar(Max),
										CompleteRoadName nvarchar(max),
										GISRoadID int,
										Error nvarchar(max)
										
									)	
	
declare 	@AddressNum int,
			@CompleteRoadName nvarchar(max),
			@GISAddressPointID nvarchar(max),
			@GISRoadID int,
			@GlobalID nvarchar(max),
			@Shape geography,
			@Low int,
			@High int,
			@RightFromAddress int,
			@RightToAddress int
			
			
select AddressNumber, 
		CompleteRoadName, 
		GISAddressPointID, 
		GISRoadID, 
		GlobalID, 
		Shape
	into #tAddressParts
	from WGS84.sde.ADDRESSPOINTS
			
declare curAddress cursor FAST_FORWARD READ_ONLY
	for (
			select 	AddressNumber, 
					CompleteRoadName, 
					GISAddressPointID, 
					GISRoadID, 
					GlobalID, 
					geography::STGeomFromText(Shape.STAsText(),4326)
			
				from #tAddressParts
		)
			

open curAddress

fetch next from curAddress into @AddressNum, @CompleteRoadName, @GISAddressPointID, @GISRoadID, @GlobalID, @Shape			
			

create table #tRoadParts (
								LeftFromAddress nvarchar(max),
								RightFromAddress nvarchar(max),
								LeftToAddress nvarchar(max),
								RightToAddress nvarchar(max),
								CompleteRoadName nvarchar(max),
								GISRoadID int,
								Shape geometry
								)

insert into #tRoadParts
select LeftFromAddress,RightFromAddress,LeftToAddress,RightToAddress,CompleteRoadName,GISRoadID,Shape
	from WGS84.sde.RoadSegment
								

while @@FETCH_STATUS = 0
	BEGIN
		print 'Processing:' + char(9) + @GlobalID + char(13) + 'At:' + char(9) + cast(GetDate() as nvarchar(max))
		begin try
			drop table #RoadCandidates
		end try
		begin catch
		end catch
		
		BEGIN TRY
				
				
				
			select top(4) GISRoadID, 
					CompleteRoadName,
					(case
							when cast(replace(replace(LeftFromAddress,' ',''),'-', '') as int) < cast(replace(replace(RightFromAddress,' ',''),'-', '') as int) then cast(replace(replace(LeftFromAddress,' ',''),'-', '') as int)
							else cast(replace(replace(RightFromAddress,' ',''),'-', '') as int)
						 END) as Low,
					(case 
							when cast(replace(replace(LeftToAddress,' ',''),'-', '') as int) > cast(replace(replace(RightToAddress,' ',''),'-', '') as int) then cast(replace(replace(LeftToAddress,' ',''),'-', '') as int)
							else cast(replace(replace(RightToAddress,' ',''),'-', '') as int)
						 END) as High,
					Shape	
				into #RoadCandidates
				from #tRoadParts
					order by (geography::STGeomFromText(Shape.STAsText(),4326)).STDistance(@Shape)
				
						
			
			set @GISRoadID = isNull(
									(
									select top(1) GISRoadID
										from #RoadCandidates
											where
												Low <= @AddressNum 
													and
												High >= @AddressNum 
													and
												CompleteRoadName = @CompleteRoadName
											
									)
									,0)
									
		END TRY
		BEGIN CATCH
			insert into GISRoadIDResults.dbo.FoundMatchesErrors(GlobalID,CompleteRoadName,GISAddressPointID,GISRoadID,Error)
				values(@GlobalID,@CompleteRoadName,@GISAddressPointID,@GISRoadID,ERROR_MESSAGE())
				
			set @GISRoadID = 0
			
			
		END CATCH
		
		BEGIN TRANSACTION
			insert into GISRoadIDResults.dbo.FoundMatches (GISAddressPointID,GISRoadID,GlobalID)
				values(@GISAddressPointID,@GISRoadID,@GlobalID)
		COMMIT TRANSACTION
		
		fetch next from curAddress into @AddressNum, @CompleteRoadName, @GISAddressPointID, @GISRoadID, @GlobalID, @Shape			
	
	END
	
	
close curAddress
deallocate curAddress
	
-----
--		Validate Results
-----
		
		
use GISRoadIDResults

select distinct GISRoadID,CompleteRoadName,Error
	from GISRoadIDResults.dbo.FoundMatchesErrors
	
	
	

select *
	from GISRoadIDResults.dbo.FoundMatches
		where GISRoadID = 0


select *
	from GISRoadIDResults.dbo.FoundMatches
		where GISRoadID <> 0
		
		

		
		
		
		
		
		