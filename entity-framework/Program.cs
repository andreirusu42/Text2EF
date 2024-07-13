
using entity_framework.Models.activity_1;

class Program
{
    static bool Test0()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Count();
        var sql_query = "SELECT count(*) FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test1()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Count();
        var sql_query = "SELECT count(*) FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test2()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Select(row => new { row.Rank }).Distinct().ToList();
        var sql_query = "SELECT DISTINCT rank FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test3()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Select(row => new { row.Rank }).Distinct().ToList();
        var sql_query = "SELECT DISTINCT rank FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test4()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Select(row => new { row.Building }).Distinct().ToList();
        var sql_query = "SELECT DISTINCT building FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test5()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Select(row => new { row.Building }).Distinct().ToList();
        var sql_query = "SELECT DISTINCT building FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test6()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Select(row => new { row.Rank, row.Fname, row.Lname }).ToList();
        var sql_query = "SELECT rank , Fname , Lname FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test7()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Select(row => new { row.Rank, row.Fname, row.Lname }).ToList();
        var sql_query = "SELECT rank , Fname , Lname FROM Faculty";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test8()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Sex == "F").Select(row => new { row.Fname, row.Lname, row.Phone }).ToList();
        var sql_query = "SELECT Fname , Lname , phone FROM Faculty WHERE Sex = 'F'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test9()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Sex == "F").Select(row => new { row.Fname, row.Lname, row.Phone }).ToList();
        var sql_query = "SELECT Fname , Lname , phone FROM Faculty WHERE Sex = 'F'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test10()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Sex == "M").Select(row => new { row.FacId }).ToList();
        var sql_query = "SELECT FacID FROM Faculty WHERE Sex = 'M'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test11()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Sex == "M").Select(row => new { row.FacId }).ToList();
        var sql_query = "SELECT FacID FROM Faculty WHERE Sex = 'M'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test12()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Sex == "F" && row.Rank == "Professor").Count();
        var sql_query = "SELECT count(*) FROM Faculty WHERE Sex = 'F' AND Rank = 'Professor'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test13()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Sex == "F" && row.Rank == "Professor").Count();
        var sql_query = "SELECT count(*) FROM Faculty WHERE Sex = 'F' AND Rank = 'Professor'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test14()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Fname == "Jerry" && row.Lname == "Prince").Select(row => new { row.Phone, row.Room, row.Building }).ToList();
        var sql_query = "SELECT phone , room , building FROM Faculty WHERE Fname = 'Jerry' AND Lname = 'Prince'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test15()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Fname == "Jerry" && row.Lname == "Prince").Select(row => new { row.Phone, row.Room, row.Building }).ToList();
        var sql_query = "SELECT phone , room , building FROM Faculty WHERE Fname = 'Jerry' AND Lname = 'Prince'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test16()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "Professor" && row.Building == "NEB").Count();
        var sql_query = "SELECT count(*) FROM Faculty WHERE Rank = 'Professor' AND building = 'NEB'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test17()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "Professor" && row.Building == "NEB").Count();
        var sql_query = "SELECT count(*) FROM Faculty WHERE Rank = 'Professor' AND building = 'NEB'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test18()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "Instructor").Select(row => new { row.Fname, row.Lname }).ToList();
        var sql_query = "SELECT fname , lname FROM Faculty WHERE Rank = 'Instructor'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test19()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "Instructor").Select(row => new { row.Fname, row.Lname }).ToList();
        var sql_query = "SELECT fname , lname FROM Faculty WHERE Rank = 'Instructor'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test20()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Building }).Select(group => new { group.Key.Building, Count = group.Count() }).ToList();
        var sql_query = "SELECT building , count(*) FROM Faculty GROUP BY building";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test21()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Building }).Select(group => new { group.Key.Building, Count = group.Count() }).ToList();
        var sql_query = "SELECT building , count(*) FROM Faculty GROUP BY building";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test22()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Building }).OrderByDescending(group => group.Count()).Select(group => new { group.Key.Building }).Take(1).ToList();
        var sql_query = "SELECT building FROM Faculty GROUP BY building ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test23()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Building }).OrderByDescending(group => group.Count()).Select(group => new { group.Key.Building }).Take(1).ToList();
        var sql_query = "SELECT building FROM Faculty GROUP BY building ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test24()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "Professor").GroupBy(row => new { row.Building }).Where(group => group.Count() >= 10).Select(group => new { group.Key.Building }).ToList();
        var sql_query = "SELECT building FROM Faculty WHERE rank = 'Professor' GROUP BY building HAVING count(*) >= 10";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test25()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "Professor").GroupBy(row => new { row.Building }).Where(group => group.Count() >= 10).Select(group => new { group.Key.Building }).ToList();
        var sql_query = "SELECT building FROM Faculty WHERE rank = 'Professor' GROUP BY building HAVING count(*) >= 10";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test26()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Rank }).Select(group => new { group.Key.Rank, Count = group.Count() }).ToList();
        var sql_query = "SELECT rank , count(*) FROM Faculty GROUP BY rank";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test27()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Rank }).Select(group => new { group.Key.Rank, Count = group.Count() }).ToList();
        var sql_query = "SELECT rank , count(*) FROM Faculty GROUP BY rank";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test28()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Rank, row.Sex }).Select(group => new { group.Key.Rank, group.Key.Sex, Count = group.Count() }).ToList();
        var sql_query = "SELECT rank , sex , count(*) FROM Faculty GROUP BY rank , sex";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test29()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Rank, row.Sex }).Select(group => new { group.Key.Rank, group.Key.Sex, Count = group.Count() }).ToList();
        var sql_query = "SELECT rank , sex , count(*) FROM Faculty GROUP BY rank , sex";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test30()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Rank }).OrderBy(group => group.Count()).Select(group => new { group.Key.Rank }).Take(1).ToList();
        var sql_query = "SELECT rank FROM Faculty GROUP BY rank ORDER BY count(*) ASC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test31()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.GroupBy(row => new { row.Rank }).OrderBy(group => group.Count()).Select(group => new { group.Key.Rank }).Take(1).ToList();
        var sql_query = "SELECT rank FROM Faculty GROUP BY rank ORDER BY count(*) ASC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test32()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "AsstProf").GroupBy(row => new { row.Sex }).Select(group => new { group.Key.Sex, Count = group.Count() }).ToList();
        var sql_query = "SELECT sex , count(*) FROM Faculty WHERE rank = 'AsstProf' GROUP BY sex";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test33()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Where(row => row.Rank == "AsstProf").GroupBy(row => new { row.Sex }).Select(group => new { group.Key.Sex, Count = group.Count() }).ToList();
        var sql_query = "SELECT sex , count(*) FROM Faculty WHERE rank = 'AsstProf' GROUP BY sex";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test34()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = 'Linda' AND T2.lname = 'Smith'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test35()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = 'Linda' AND T2.lname = 'Smith'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test36()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T1.Rank == "Professor").Select(row => new { row.T2.StuId }).ToList();
        var sql_query = "SELECT T2.StuID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T1.rank = 'Professor'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test37()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T1.Rank == "Professor").Select(row => new { row.T2.StuId }).ToList();
        var sql_query = "SELECT T2.StuID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T1.rank = 'Professor'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test38()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T1.Fname == "Michael" && row.T1.Lname == "Goodrich").Select(row => new { row.T2.Fname, row.T2.Lname }).ToList();
        var sql_query = "SELECT T2.fname , T2.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T1.fname = 'Michael' AND T1.lname = 'Goodrich'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test39()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T1.Fname == "Michael" && row.T1.Lname == "Goodrich").Select(row => new { row.T2.Fname, row.T2.Lname }).ToList();
        var sql_query = "SELECT T2.fname , T2.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T1.fname = 'Michael' AND T1.lname = 'Goodrich'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test40()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.Key.FacId, Count = group.Count() }).ToList();
        var sql_query = "SELECT T1.FacID , count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test41()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.Key.FacId, Count = group.Count() }).ToList();
        var sql_query = "SELECT T1.FacID , count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test42()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Rank }).Select(group => new { group.Key.Rank, Count = group.Count() }).ToList();
        var sql_query = "SELECT T1.rank , count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.rank";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test43()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Rank }).Select(group => new { group.Key.Rank, Count = group.Count() }).ToList();
        var sql_query = "SELECT T1.rank , count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.rank";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test44()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test45()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test46()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Where(group => group.Count() >= 2).Select(group => new { group.Key.FacId }).ToList();
        var sql_query = "SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID HAVING count(*) >= 2";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test47()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Where(group => group.Count() >= 2).Select(group => new { group.Key.FacId }).ToList();
        var sql_query = "SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID HAVING count(*) >= 2";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test48()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Select(row => new { row.ActivityName }).ToList();
        var sql_query = "SELECT activity_name FROM Activity";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test49()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Select(row => new { row.ActivityName }).ToList();
        var sql_query = "SELECT activity_name FROM Activity";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test50()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Count();
        var sql_query = "SELECT count(*) FROM Activity";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test51()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Count();
        var sql_query = "SELECT count(*) FROM Activity";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test52()
    {
        var context = new Activity1Context();
        var linq_query = context.FacultyParticipatesIns.Select(row => row.FacId).Distinct().Count();
        var sql_query = "SELECT count(DISTINCT FacID) FROM Faculty_participates_in";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test53()
    {
        var context = new Activity1Context();
        var linq_query = context.FacultyParticipatesIns.Select(row => row.FacId).Distinct().Count();
        var sql_query = "SELECT count(DISTINCT FacID) FROM Faculty_participates_in";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test54()
    {
        var context = new Activity1Context();
        var linq_query = context.FacultyParticipatesIns.Select(row => row.FacId).Intersect(context.Students.Select(row => row.Advisor)).ToList();
        var sql_query = "SELECT FacID FROM Faculty_participates_in INTERSECT SELECT advisor FROM Student";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test55()
    {
        var context = new Activity1Context();
        var linq_query = context.FacultyParticipatesIns.Select(row => row.FacId).Intersect(context.Students.Select(row => row.Advisor)).ToList();
        var sql_query = "SELECT FacID FROM Faculty_participates_in INTERSECT SELECT advisor FROM Student";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test56()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).Where(row => row.T1.Fname == "Mark" && row.T1.Lname == "Giuliano").Count();
        var sql_query = "SELECT count(*) FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID WHERE T1.fname = 'Mark' AND T1.lname = 'Giuliano'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test57()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).Where(row => row.T1.Fname == "Mark" && row.T1.Lname == "Giuliano").Count();
        var sql_query = "SELECT count(*) FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID WHERE T1.fname = 'Mark' AND T1.lname = 'Giuliano'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test58()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).Join(context.Activities, joined => joined.T2.Actid, T3 => T3.Actid, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T1.Fname == "Mark" && row.T1.Lname == "Giuliano").Select(row => new { row.T3.ActivityName }).ToList();
        var sql_query = "SELECT T3.activity_name FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID JOIN Activity AS T3 ON T3.actid = T2.actid WHERE T1.fname = 'Mark' AND T1.lname = 'Giuliano'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test59()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).Join(context.Activities, joined => joined.T2.Actid, T3 => T3.Actid, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T1.Fname == "Mark" && row.T1.Lname == "Giuliano").Select(row => new { row.T3.ActivityName }).ToList();
        var sql_query = "SELECT T3.activity_name FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID JOIN Activity AS T3 ON T3.actid = T2.actid WHERE T1.fname = 'Mark' AND T1.lname = 'Giuliano'";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test60()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.First().T1.Fname, group.First().T1.Lname, Count = group.Count(), group.Key.FacId }).ToList();
        var sql_query = "SELECT T1.fname , T1.lname , count(*) , T1.FacID FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID GROUP BY T1.FacID";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test61()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.First().T1.Fname, group.First().T1.Lname, Count = group.Count(), group.Key.FacId }).ToList();
        var sql_query = "SELECT T1.fname , T1.lname , count(*) , T1.FacID FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID GROUP BY T1.FacID";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test62()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).Select(group => new { group.First().T1.ActivityName, Count = group.Count() }).ToList();
        var sql_query = "SELECT T1.activity_name , count(*) FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID = T2.actID GROUP BY T1.actID";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test63()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).Select(group => new { group.First().T1.ActivityName, Count = group.Count() }).ToList();
        var sql_query = "SELECT T1.activity_name , count(*) FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID = T2.actID GROUP BY T1.actID";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test64()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test65()
    {
        var context = new Activity1Context();
        var linq_query = context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID = T2.facID GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test66()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.ActivityName }).Take(1).ToList();
        var sql_query = "SELECT T1.activity_name FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID = T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test67()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.ActivityName }).Take(1).ToList();
        var sql_query = "SELECT T1.activity_name FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID = T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test68()
    {
        var context = new Activity1Context();
        var linq_query = context.ParticipatesIns.Select(row => row.Stuid).Intersect(context.Students.Where(row => row.Age < 20).Select(row => row.StuId)).ToList();
        var sql_query = "SELECT StuID FROM Participates_in INTERSECT SELECT StuID FROM Student WHERE age < 20";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test69()
    {
        var context = new Activity1Context();
        var linq_query = context.ParticipatesIns.Select(row => row.Stuid).Intersect(context.Students.Where(row => row.Age < 20).Select(row => row.StuId)).ToList();
        var sql_query = "SELECT StuID FROM Participates_in INTERSECT SELECT StuID FROM Student WHERE age < 20";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test70()
    {
        var context = new Activity1Context();
        var linq_query = context.Students.Join(context.ParticipatesIns, T1 => T1.StuId, T2 => T2.Stuid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.StuId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Student AS T1 JOIN Participates_in AS T2 ON T1.StuID = T2.StuID GROUP BY T1.StuID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test71()
    {
        var context = new Activity1Context();
        var linq_query = context.Students.Join(context.ParticipatesIns, T1 => T1.StuId, T2 => T2.Stuid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.StuId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();
        var sql_query = "SELECT T1.fname , T1.lname FROM Student AS T1 JOIN Participates_in AS T2 ON T1.StuID = T2.StuID GROUP BY T1.StuID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test72()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Join(context.ParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.ActivityName }).Take(1).ToList();
        var sql_query = "SELECT T1.activity_name FROM Activity AS T1 JOIN Participates_in AS T2 ON T1.actID = T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static bool Test73()
    {
        var context = new Activity1Context();
        var linq_query = context.Activities.Join(context.ParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.ActivityName }).Take(1).ToList();
        var sql_query = "SELECT T1.activity_name FROM Activity AS T1 JOIN Participates_in AS T2 ON T1.actID = T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1";

        var test_passed = Tester.Test(linq_query, sql_query);

        return test_passed;
    }

    static void Main()
    {

        var test_passed_0 = Test0();
        if (!test_passed_0)
        {
            Console.WriteLine("Test 0 failed");
            return;
        }

        var test_passed_1 = Test1();
        if (!test_passed_1)
        {
            Console.WriteLine("Test 1 failed");
            return;
        }

        var test_passed_2 = Test2();
        if (!test_passed_2)
        {
            Console.WriteLine("Test 2 failed");
            return;
        }

        var test_passed_3 = Test3();
        if (!test_passed_3)
        {
            Console.WriteLine("Test 3 failed");
            return;
        }

        var test_passed_4 = Test4();
        if (!test_passed_4)
        {
            Console.WriteLine("Test 4 failed");
            return;
        }

        var test_passed_5 = Test5();
        if (!test_passed_5)
        {
            Console.WriteLine("Test 5 failed");
            return;
        }

        var test_passed_6 = Test6();
        if (!test_passed_6)
        {
            Console.WriteLine("Test 6 failed");
            return;
        }

        var test_passed_7 = Test7();
        if (!test_passed_7)
        {
            Console.WriteLine("Test 7 failed");
            return;
        }

        var test_passed_8 = Test8();
        if (!test_passed_8)
        {
            Console.WriteLine("Test 8 failed");
            return;
        }

        var test_passed_9 = Test9();
        if (!test_passed_9)
        {
            Console.WriteLine("Test 9 failed");
            return;
        }

        var test_passed_10 = Test10();
        if (!test_passed_10)
        {
            Console.WriteLine("Test 10 failed");
            return;
        }

        var test_passed_11 = Test11();
        if (!test_passed_11)
        {
            Console.WriteLine("Test 11 failed");
            return;
        }

        var test_passed_12 = Test12();
        if (!test_passed_12)
        {
            Console.WriteLine("Test 12 failed");
            return;
        }

        var test_passed_13 = Test13();
        if (!test_passed_13)
        {
            Console.WriteLine("Test 13 failed");
            return;
        }

        var test_passed_14 = Test14();
        if (!test_passed_14)
        {
            Console.WriteLine("Test 14 failed");
            return;
        }

        var test_passed_15 = Test15();
        if (!test_passed_15)
        {
            Console.WriteLine("Test 15 failed");
            return;
        }

        var test_passed_16 = Test16();
        if (!test_passed_16)
        {
            Console.WriteLine("Test 16 failed");
            return;
        }

        var test_passed_17 = Test17();
        if (!test_passed_17)
        {
            Console.WriteLine("Test 17 failed");
            return;
        }

        var test_passed_18 = Test18();
        if (!test_passed_18)
        {
            Console.WriteLine("Test 18 failed");
            return;
        }

        var test_passed_19 = Test19();
        if (!test_passed_19)
        {
            Console.WriteLine("Test 19 failed");
            return;
        }

        var test_passed_20 = Test20();
        if (!test_passed_20)
        {
            Console.WriteLine("Test 20 failed");
            return;
        }

        var test_passed_21 = Test21();
        if (!test_passed_21)
        {
            Console.WriteLine("Test 21 failed");
            return;
        }

        var test_passed_22 = Test22();
        if (!test_passed_22)
        {
            Console.WriteLine("Test 22 failed");
            return;
        }

        var test_passed_23 = Test23();
        if (!test_passed_23)
        {
            Console.WriteLine("Test 23 failed");
            return;
        }

        var test_passed_24 = Test24();
        if (!test_passed_24)
        {
            Console.WriteLine("Test 24 failed");
            return;
        }

        var test_passed_25 = Test25();
        if (!test_passed_25)
        {
            Console.WriteLine("Test 25 failed");
            return;
        }

        var test_passed_26 = Test26();
        if (!test_passed_26)
        {
            Console.WriteLine("Test 26 failed");
            return;
        }

        var test_passed_27 = Test27();
        if (!test_passed_27)
        {
            Console.WriteLine("Test 27 failed");
            return;
        }

        var test_passed_28 = Test28();
        if (!test_passed_28)
        {
            Console.WriteLine("Test 28 failed");
            return;
        }

        var test_passed_29 = Test29();
        if (!test_passed_29)
        {
            Console.WriteLine("Test 29 failed");
            return;
        }

        var test_passed_30 = Test30();
        if (!test_passed_30)
        {
            Console.WriteLine("Test 30 failed");
            return;
        }

        var test_passed_31 = Test31();
        if (!test_passed_31)
        {
            Console.WriteLine("Test 31 failed");
            return;
        }

        var test_passed_32 = Test32();
        if (!test_passed_32)
        {
            Console.WriteLine("Test 32 failed");
            return;
        }

        var test_passed_33 = Test33();
        if (!test_passed_33)
        {
            Console.WriteLine("Test 33 failed");
            return;
        }

        var test_passed_34 = Test34();
        if (!test_passed_34)
        {
            Console.WriteLine("Test 34 failed");
            return;
        }

        var test_passed_35 = Test35();
        if (!test_passed_35)
        {
            Console.WriteLine("Test 35 failed");
            return;
        }

        var test_passed_36 = Test36();
        if (!test_passed_36)
        {
            Console.WriteLine("Test 36 failed");
            return;
        }

        var test_passed_37 = Test37();
        if (!test_passed_37)
        {
            Console.WriteLine("Test 37 failed");
            return;
        }

        var test_passed_38 = Test38();
        if (!test_passed_38)
        {
            Console.WriteLine("Test 38 failed");
            return;
        }

        var test_passed_39 = Test39();
        if (!test_passed_39)
        {
            Console.WriteLine("Test 39 failed");
            return;
        }

        var test_passed_40 = Test40();
        if (!test_passed_40)
        {
            Console.WriteLine("Test 40 failed");
            return;
        }

        var test_passed_41 = Test41();
        if (!test_passed_41)
        {
            Console.WriteLine("Test 41 failed");
            return;
        }

        var test_passed_42 = Test42();
        if (!test_passed_42)
        {
            Console.WriteLine("Test 42 failed");
            return;
        }

        var test_passed_43 = Test43();
        if (!test_passed_43)
        {
            Console.WriteLine("Test 43 failed");
            return;
        }

        var test_passed_44 = Test44();
        if (!test_passed_44)
        {
            Console.WriteLine("Test 44 failed");
            return;
        }

        var test_passed_45 = Test45();
        if (!test_passed_45)
        {
            Console.WriteLine("Test 45 failed");
            return;
        }

        var test_passed_46 = Test46();
        if (!test_passed_46)
        {
            Console.WriteLine("Test 46 failed");
            return;
        }

        var test_passed_47 = Test47();
        if (!test_passed_47)
        {
            Console.WriteLine("Test 47 failed");
            return;
        }

        var test_passed_48 = Test48();
        if (!test_passed_48)
        {
            Console.WriteLine("Test 48 failed");
            return;
        }

        var test_passed_49 = Test49();
        if (!test_passed_49)
        {
            Console.WriteLine("Test 49 failed");
            return;
        }

        var test_passed_50 = Test50();
        if (!test_passed_50)
        {
            Console.WriteLine("Test 50 failed");
            return;
        }

        var test_passed_51 = Test51();
        if (!test_passed_51)
        {
            Console.WriteLine("Test 51 failed");
            return;
        }

        var test_passed_52 = Test52();
        if (!test_passed_52)
        {
            Console.WriteLine("Test 52 failed");
            return;
        }

        var test_passed_53 = Test53();
        if (!test_passed_53)
        {
            Console.WriteLine("Test 53 failed");
            return;
        }

        var test_passed_54 = Test54();
        if (!test_passed_54)
        {
            Console.WriteLine("Test 54 failed");
            return;
        }

        var test_passed_55 = Test55();
        if (!test_passed_55)
        {
            Console.WriteLine("Test 55 failed");
            return;
        }

        var test_passed_56 = Test56();
        if (!test_passed_56)
        {
            Console.WriteLine("Test 56 failed");
            return;
        }

        var test_passed_57 = Test57();
        if (!test_passed_57)
        {
            Console.WriteLine("Test 57 failed");
            return;
        }

        var test_passed_58 = Test58();
        if (!test_passed_58)
        {
            Console.WriteLine("Test 58 failed");
            return;
        }

        var test_passed_59 = Test59();
        if (!test_passed_59)
        {
            Console.WriteLine("Test 59 failed");
            return;
        }

        var test_passed_60 = Test60();
        if (!test_passed_60)
        {
            Console.WriteLine("Test 60 failed");
            return;
        }

        var test_passed_61 = Test61();
        if (!test_passed_61)
        {
            Console.WriteLine("Test 61 failed");
            return;
        }

        var test_passed_62 = Test62();
        if (!test_passed_62)
        {
            Console.WriteLine("Test 62 failed");
            return;
        }

        var test_passed_63 = Test63();
        if (!test_passed_63)
        {
            Console.WriteLine("Test 63 failed");
            return;
        }

        var test_passed_64 = Test64();
        if (!test_passed_64)
        {
            Console.WriteLine("Test 64 failed");
            return;
        }

        var test_passed_65 = Test65();
        if (!test_passed_65)
        {
            Console.WriteLine("Test 65 failed");
            return;
        }

        var test_passed_66 = Test66();
        if (!test_passed_66)
        {
            Console.WriteLine("Test 66 failed");
            return;
        }

        var test_passed_67 = Test67();
        if (!test_passed_67)
        {
            Console.WriteLine("Test 67 failed");
            return;
        }

        var test_passed_68 = Test68();
        if (!test_passed_68)
        {
            Console.WriteLine("Test 68 failed");
            return;
        }

        var test_passed_69 = Test69();
        if (!test_passed_69)
        {
            Console.WriteLine("Test 69 failed");
            return;
        }

        var test_passed_70 = Test70();
        if (!test_passed_70)
        {
            Console.WriteLine("Test 70 failed");
            return;
        }

        var test_passed_71 = Test71();
        if (!test_passed_71)
        {
            Console.WriteLine("Test 71 failed");
            return;
        }

        var test_passed_72 = Test72();
        if (!test_passed_72)
        {
            Console.WriteLine("Test 72 failed");
            return;
        }

        var test_passed_73 = Test73();
        if (!test_passed_73)
        {
            Console.WriteLine("Test 73 failed");
            return;
        }
    }
}