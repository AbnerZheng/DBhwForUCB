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
	SELECT 1, 1, 1, 1, 1 -- replace this line
	;

	-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
	AS
	SELECT 1, 1, 1, 1 -- replace this line
	;

	-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
	AS
	SELECT 1, 1, 1, 1 -- replace this line
	;

	-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
	AS
	SELECT 1, 1, 1, 1, 1 -- replace this line
	;

