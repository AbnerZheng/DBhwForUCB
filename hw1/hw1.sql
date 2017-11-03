DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv;

	-- Question 0
CREATE VIEW q0(era) 
	AS
SELECT MAX(era)
	FROM pitching 
	;

	-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
	AS
	SELECT namefirst,namelast, birthyear from master where weight > 300
	;

	-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
	AS
	SELECT namefirst, namelast, birthyear from master where namefirst like '% %'
	;

	-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
	AS
	SELECT birthyear, avg(height), count(1) from master group by birthyear order by birthyear
	;

	-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
	AS
SELECT birthyear, avg(height), count(1)
	from master
	group by birthyear
	having avg(height) > 70
	order by birthyear
	;

	-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
	AS
	SELECT namefirst, namelast, playerid, yearid
	from master
	natural join halloffame
	where halloffame.inducted = 'Y'
	order by yearid desc
	;

	-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
	AS
	SELECT M.namefirst, M.namelast,M.playerid, S.schoolid, H.yearid
	from master as M
	join halloffame as H
	on M.playerid = H.playerid
	join collegeplaying as C
	on C.playerid = M.playerid
	join schools as S
	on S.schoolid = C.schoolid
	where H.inducted = 'Y'
	and S.schoolstate = 'CA'
	order by H.yearid desc, S.schoolid, M.playerid
	;

	-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
	AS
	select master.playerid, master.namefirst,
	master.namelast, collegeplaying.schoolid 
	from master
	join halloffame
	on master.playerid = halloffame.playerid
	left join collegeplaying
	on collegeplaying.playerid = master.playerid
	where halloffame.inducted = 'Y'
	order by master.playerid desc, collegeplaying.schoolid
	;


	-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
	AS
	select master.playerid, namefirst, namelast, yearid,(B.H + B.h2B + 2*B.h3B + 3*B.HR)::float / B.AB as slg
	from batting B, master
	where B.AB > 50 and master.playerid = B.playerid
	order by slg desc, yearid, playerid
	limit 10
	;

	-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
	AS
	select master.playerid, master.namefirst, master.namelast,T.lslg
	from master
	join (select playerid, sum(B.H + B.h2B + 2*B.h3B + 3 * B.HR)::float/ sum(B.AB) as lslg
			from batting B
			group by B.playerid
			having sum(B.AB) > 50
			order by lslg desc limit 10)T
	on T.playerid = master.playerid
	;

	-- Question 3iii

CREATE VIEW q3iii(namefirst, namelast, lslg)
	AS
	select master.namefirst, master.namelast, sub2.lslg
	from master, (select playerid, sum(B.H + B.h2b + 2 * B.h3b + 3*B.hr)::float / sum(B.AB) as lslg
			from batting B 
			where B.playerid = 'mayswi01'
			group by B.playerid) sub1, (select playerid, sum(B.H + B.h2b + 2 * B.h3b + 3*B.hr)::float / sum(B.AB) as lslg
				from batting B 
				group by B.playerid
				having sum(B.AB)>50
				) sub2
			where sub1.lslg < sub2.lslg and master.playerid = sub2.playerid
			;

	-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
    AS
    select yearid, min(salary), max(salary), avg(salary), stddev(salary) from salaries group by yearid order by yearid
    ;

    -- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
    AS
    select bucket, bucket * (sub3.max-sub3.min)/10 + sub3.min, (bucket+1) * (sub3.max-sub3.min)/10 + sub3.min, sub2.cnt
    from(
    select
    case when salary = sub1.max then 9 else floor((salary-sub1.min)/interval) end
    as bucket, count(1) as cnt
    from salaries, (
    select min, max, (max-min)/10 as interval from q4i where yearid=2016
    )sub1
    where yearid=2016
    group by bucket
    ) sub2, (select * from q4i where yearid=2016) sub3
    order by bucket
    ;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
select t2.yearid, t2.min-t1.min as mindiff,t2.max-t1.max as maxdiff,t2.avg-t1.avg as avgdiff  from q4i t1, q4i t2 where t2.yearid - t1.yearid = 1 order by t2.yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
select master.playerid, namefirst, namelast, sub1.salary,sub1.yearid from (salaries join (select yearid, max(salary) as salary from salaries where yearid = 2000 or yearid = 2001group by yearid) sub1 on sub1.yearid = salaries.yearid and sub1.salary = salaries.salary) join master on master.playerid = salaries.playerid;
;
