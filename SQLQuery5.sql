select 'Attendance Detail Staging Mandatory Before',count(*) from [dbo].[seats.Attendance_Detail_Staging_Mandatory]
select 'Attendance Detail Staging Optional Before',count(*) from [dbo].[seats.Attendance_Detail_Staging_Optional]
select distinct attendancedate, count(*) as records from [dbo].[seats.Attendance_Detail] group by attendancedate order by attendancedate desc

truncate table [dbo].[seats.Attendance_Detail_Staging_Mandatory]
truncate table [dbo].[seats.Attendance_Detail_Staging_Optional]

BULK INSERT [dbo].[seats.Attendance_Detail_Staging_Mandatory]
FROM 'D:\SEATS\Student Attendance Report Mandatory.csv'
WITH (FORMAT='CSV', FIRSTROW=3)

BULK INSERT [dbo].[seats.Attendance_Detail_Staging_Optional]
FROM 'D:\SEATS\Student Attendance Report Optional.csv'
WITH (FORMAT='CSV', FIRSTROW=3)

select 'Attendance Detail Staging After',count(*) from [dbo].[seats.Attendance_Detail_Staging]

truncate table [dbo].[seats.Attendance_Detail_Staging_Optional]
BULK INSERT [dbo].[seats.Attendance_Detail_Staging_Optional]
FROM 'D:\SEATS\Student Attendance Report Optional.csv'
WITH (FORMAT='CSV', FIRSTROW=3)

select 'Attendance Detail Staging Mandatory After',count(*) from [dbo].[seats.Attendance_Detail_Staging_Mandatory]
select 'Attendance Detail Staging Optional After',count(*) from [dbo].[seats.Attendance_Detail_Staging_Optional]

select 'Attendance Detail Before', count(*) from [dbo].[seats.Attendance_Detail_2324]

truncate table [dbo].[seats.Attendance_Detail_2324]
insert into [dbo].[seats.Attendance_Detail_1234]
(
  [AttendanceType],
  [StudentID],
  [Firstname],
  [Surname],
  [Room],
  [Attendancedate],
  [Coursecode],
  [Module],
  [Lecturer],
  [Coursegroup],
  [StartTime],
  [EndTime],
  [Swipetime],
  [Attendancepercent],
  [MandatoryOptional]
)
select
  [AttendanceType],
  [StudentID],
  [Firstname],
  [Surname],
  [Room],
  [Attendancedate],
  [Coursecode],
  [Module],
  [Lecturer],
  [Coursegroup],
  [StartTime],
  [EndTime],
  [Swipetime],
  [Attendancepercent],
  [MandatoryOptional]
from [dbo].[seats.Attendance_Detail_Staging_Mandatory]

UNION ALL

select
  [AttendanceType],
  [StudentID],
  [Firstname],
  [Surname],
  [Room],
  [Attendancedate],
  [Coursecode],
  [Module],
  NULL as [Lecturer],
  NULL as [Coursegroup],
  NULL as [StartTime],
  NULL as [EndTime],
  NULL as [Swipetime],
  NULL as [Attendancepercent],
  NULL as [MandatoryOptional]
from [dbo].[seats.Attendance_Detail_Staging_Optional]

go

select 'Attendance Detail After', count(*) 
from [dbo].[seats.Attendance_Detail_1234]

go

-- Just to make sure there's data in for the dates expected
select distinct attendancedate, mandatoryoptional, coursegroup, count(*) as records 
from [dbo].[seats.Attendance_Detail_1234] 
group by attendancedate, mandatoryoptional, coursegroup 
order by attendancedate desc
go
