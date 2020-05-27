import model.Job
import model.JobDataSource
import org.jsoup.Jsoup
import scala.jdk.CollectionConverters._

object Application {
  def main(args: Array[String]): Unit = {

    // Scrape Linkedin for jobs
    val LINKEDIN_API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Software%2BEngineer&location=San%2BFrancisco%2C%2BCalifornia%2C%2BUnited%2BStates&trk=public_jobs_jobs-search-bar_search-submit&start="
    val start = 1
    val doc = Jsoup.connect(LINKEDIN_API_URL + start).get()
    val jobCards = doc.select("li.result-card.job-result-card")
    val results = jobCards.asScala.map(jobPost => {
      val company = jobPost.select("h4 a").text
      val jobTitle = jobPost.select("a").text
      val url = jobPost.select("a").attr("href")
      List(company, jobTitle, url)
    }).toList

    // Save to DataSource
    val datasource = new JobDataSource
    if (datasource.open()) {
      for (List(company, jobTitle, url) <- results) {
        val job = Job(company, jobTitle, url)
        datasource.insertJob(job) match {
          case Some(id) => println(s"Inserted $jobTitle at $company with ID $id")
          case None => println("An error occurred")
        }
      }
      datasource.close()
    }
  }
}
