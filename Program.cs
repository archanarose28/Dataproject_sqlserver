using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using CsvHelper;
using ConsoleTables;
using System.Globalization;
namespace DataProject
{

    public class CompanyMas
    {
        public string AUTHORIZED_CAP { get; set; }
        public string DATE_OF_REGISTRATION { get; set; }
        public string PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            string InputStr;
            InputStr = Console.ReadLine();
            if (InputStr == "load")    //loader starts
            {
                //establishing connection
                SqlConnection con;
                SqlCommand cmd;

                con = new SqlConnection("server=.;integrated security=SSPI;MultipleActiveResultSets=true;");
                con.Open();

                string command;
                command = "SELECT * FROM master.sys.databases WHERE name = N'myDatabase' ";
                cmd = new SqlCommand(command, con);
                var existCheck = cmd.ExecuteReader();

                if (existCheck.Read())                       //1.  if database already  exists.
                {
                    existCheck.Close();
                    Console.WriteLine("Database exists!");


                    command = "USE myDatabase " + "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'COMPANYTABLE'";
                    //  command = "IF OBJECT_ID('*COMPANYTABLE', 'U') IS NOT NULL";
                    cmd = new SqlCommand(command, con);
                    var existChecks = cmd.ExecuteReader();

                    //2.Create the table if it does not exist.
                    if ((existChecks.Read()))
                    {
                        Console.WriteLine("Table exists!!");

                    }
                    else
                    {
                        Console.WriteLine("Table doesnot exist!!");
                        command = "USE myDatabase  " + "CREATE TABLE COMPANYTABLE(" + "AUTHORIZED_CAP   decimal(15,0) NOT NULL,"
                       + "DATE_OF_REGISTRATION  int, PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN varchar(200)" + ")";
                        cmd = new SqlCommand(command, con);
                        existChecks = cmd.ExecuteReader();


                        Console.WriteLine("Table is Created");

                    } //
                    int flag = 0;
                    if (flag != 0)  //since now values are already in table so setting flag
                    {
                        //3. Read the CSV file and add the data from the CSV file to the database.
                        //command = "SELECT COUNT(*) FROM myDatabase.dbo.COMPANY ";   will return count of rows
                        using (var reader = new StreamReader("C:\\Users\\ARCHANA ROSE BIJU\\Downloads\\Tripura.csv"))
                        {
                            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                            {
                                csv.Read();
                                csv.ReadHeader();
                                while (csv.Read())
                                {

                                    var record = csv.GetRecord<CompanyMas>();
                                    decimal k = Convert.ToDecimal(record.AUTHORIZED_CAP); //authorized cap as decimal
                                    string dateStr = record.DATE_OF_REGISTRATION; //each year as int
                                    DateTime checkDate = Convert.ToDateTime(dateStr);
                                    int eachYear = checkDate.Year;
                                    string pba = record.PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN;
                                    command = "use myDatabase " + "INSERT INTO myDatabase.dbo.COMPANYTABLE VALUES(' " + k + " ',' " + eachYear + " ',' " + pba + " ')";
                                    cmd = new SqlCommand(command, con);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            Console.WriteLine("VALUES INSERTED INTO TABLE");
                        }
                    }
                    else
                    {
                        Console.WriteLine("VALUES ALREADY EXISTS IN TABLE");
                    }
                }
                else
                {
                    //1. Create the database if it does not exist.

                    Console.WriteLine("Database does not exist!");
                    existCheck.Close();
                    command = "CREATE DATABASE myDatabase";
                    cmd = new SqlCommand(command, con);
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Database is Created");
                    //   con.Close();

                }
            } //loader finished
            else if (InputStr == "analyze")  //analyzer starts
            {
                //problem1
                int valOne = 0;
                int valTwo = 0;
                int valThree = 0;
                int valFour = 0;
                int valFive = 0;
                SqlConnection con;
                SqlCommand cmd;

                con = new SqlConnection("server=.;integrated security=SSPI;MultipleActiveResultSets=true;database=myDatabase;");
                con.Open();

                string command;
                command = ("SELECT count(*) FROM COMPANYTABLE WHERE AUTHORIZED_CAP<=100000");
                cmd = new SqlCommand(command, con);
                valOne = (Int32)cmd.ExecuteScalar();
                command = ("SELECT count(*)   FROM COMPANYTABLE WHERE(AUTHORIZED_CAP > 100000 AND AUTHORIZED_CAP <= 1000000)");
                cmd = new SqlCommand(command, con);
                valTwo = (Int32)cmd.ExecuteScalar();
                command = ("SELECT count(*) FROM COMPANYTABLE WHERE (AUTHORIZED_CAP>1000000 AND AUTHORIZED_CAP<=10000000)");
                cmd = new SqlCommand(command, con);
                valThree = (Int32)cmd.ExecuteScalar();
                command = ("SELECT count(*) FROM COMPANYTABLE WHERE (AUTHORIZED_CAP>10000000 AND AUTHORIZED_CAP<=100000000)");
                cmd = new SqlCommand(command, con);
                valFour = (Int32)cmd.ExecuteScalar();
                command = ("SELECT count(*) FROM COMPANYTABLE  WHERE(AUTHORIZED_CAP > 100000000)");
                cmd = new SqlCommand(command, con);
                valFive = (Int32)cmd.ExecuteScalar();




                var table = new ConsoleTable("Bin", "Counts");
                table.AddRow("<= 1L", valOne).AddRow(" 1L to 10L", valTwo).AddRow("10L to 1Cr ", valThree)
                    .AddRow("1Cr to 10Cr ", valFour).AddRow(">10Cr ", valFive);
                table.Write();
                //problem2

                table = new ConsoleTable("Year", "Count");

                command = " select DATE_OF_REGISTRATION, count(DATE_OF_REGISTRATION) as 'Count' from COMPANYTABLE WHERE DATE_OF_REGISTRATION >= 2000 AND DATE_OF_REGISTRATION<= 2019"
                + "GROUP BY DATE_OF_REGISTRATION";
                cmd = new SqlCommand(command, con);

                var readEq = cmd.ExecuteReader();
                String year, count;
                while (readEq.Read())
                {
                    year = readEq["DATE_OF_REGISTRATION"].ToString();
                    count = readEq["Count"].ToString();
                    table.AddRow(year, count);
                }
                table.Write();

                //problem3
                table = new ConsoleTable("PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN ", "Count");
                command = "SELECT PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN, COUNT(PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN) AS 'Count' from COMPANYTABLE"
                + "  WHERE DATE_OF_REGISTRATION = 2015 " + " GROUP BY PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN";
                cmd = new SqlCommand(command, con);

                readEq = cmd.ExecuteReader();
                String pba, counts;
                while (readEq.Read())
                {
                    pba = readEq["PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN"].ToString();
                    counts = readEq["Count"].ToString();
                    table.AddRow(pba, counts);
                }
                table.Write();

                //problem4

                table = new ConsoleTable("PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN ", "Counts");
                command = "SELECT PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN , DATE_OF_REGISTRATION, COUNT(PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN) "
                   + " AS 'Count' from COMPANYTABLE WHERE DATE_OF_REGISTRATION >= 2000 AND  DATE_OF_REGISTRATION <= 2019"
                    + "GROUP BY PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN , DATE_OF_REGISTRATION";


                cmd = new SqlCommand(command, con);
                string eachyear;
                readEq = cmd.ExecuteReader();
                string dateF = "2000";
                table.AddRow(dateF, "--");
                while (readEq.Read()) //each row
                {

                    pba = readEq["PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN"].ToString();
                    counts = readEq["Count"].ToString();
                    eachyear = readEq["DATE_OF_REGISTRATION"].ToString();
                    if (eachyear == dateF)
                    {
                        table.AddRow(pba, counts);
                    }
                    else if (eachyear != dateF)
                    {
                        dateF = eachyear;
                        table.AddRow(dateF, "--");
                        table.AddRow(pba, counts);
                    }
                }
                table.Write();
            }
            Console.ReadLine();

        }
    }
}

