package model

import java.sql._

class JobDataSource {
  type DatabaseId = Int

  private val DB_NAME = "jobs.db"
  private val CONNECTION_STRING = "jdbc:sqlite:/Users/julianmclain/code/scala-scraper/" + DB_NAME
  private val QUERY_TIMEOUT = 10

  private val TABLE_NAME = "jobs"
  private val COLUMN_JOB_ID = "job_id"
  private val COLUMN_COMPANY_NAME = "company_name"
  private val COLUMN_JOB_TITLE = "job_title"
  private val COLUMN_URL = "url"

  private val CREATE_TABLE_STRING = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
    "(" + COLUMN_JOB_ID + " INTEGER NOT NULL PRIMARY KEY, " +
    COLUMN_COMPANY_NAME + " TEXT, " +
    COLUMN_JOB_TITLE + " TEXT, " +
    COLUMN_URL + " TEXT" +
    ")"

  private val INSERT_JOB_STRING = s"INSERT INTO $TABLE_NAME ($COLUMN_COMPANY_NAME, " +
    s"$COLUMN_JOB_TITLE, $COLUMN_URL) VALUES(?, ?, ?)"

  // TODO: There has to be a better way to do this
  private var conn: Connection = _
  private var insertJobStatement: PreparedStatement = _
  private var statement: Statement = _

  def open(): Boolean = {
    try {
      conn = DriverManager.getConnection(CONNECTION_STRING)
      statement = conn.createStatement()
      statement.setQueryTimeout(QUERY_TIMEOUT)
      statement.executeUpdate(CREATE_TABLE_STRING)
      insertJobStatement = conn.prepareStatement(INSERT_JOB_STRING, Statement.RETURN_GENERATED_KEYS)
      true
    } catch {
      case e: SQLException => {
        println("Couldn't connect to datasource: " + e.getMessage)
        false
      }
    }
  }

  def insertJob(job: Job): Option[DatabaseId] = {
    insertJobStatement.setString(1, job.companyName)
    insertJobStatement.setString(2, job.jobTitle)
    insertJobStatement.setString(3, job.url)
    val affectedRows = insertJobStatement.executeUpdate()
    if (affectedRows !=1) {
      println("Unable to insert job")
      None
    }
    else {
      // TODO: More idiomatic way to handle ResultSet https://stackoverflow.com/questions/9636545/treating-an-sql-resultset-like-a-scala-stream
      val generatedKeys: ResultSet = insertJobStatement.getGeneratedKeys()
      if (generatedKeys.next()) {
        Some(generatedKeys.getInt(1))
      }
      else {
        println("Unable to get inserted job id")
        None
      }
    }
  }

  def close(): Boolean = {
    try {
      if (insertJobStatement != null) insertJobStatement.close()
      if (conn != null) conn.close()
      true
    } catch {
      case e: SQLException => {
        println("An error occurred while closing the datasource connection: " + e.getMessage)
        false
      }
    }
  }
}

