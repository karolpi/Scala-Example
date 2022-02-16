import JsonShopScraper.defaultRetrySettings
import akka.NotUsed
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import io.circe.Decoder.Result
import io.circe._
import io.circe.optics.JsonPath.root
import io.circe.optics._
import io.circe.parser._

import scala.collection.immutable.Iterable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

trait JsonShopScraper {

  /**
   * Transforms json to Product. Can be used as parameter in [[productListDecoder]]
   * @param pageDetails Optional PageDetails parameter used to construct Product.
   */
  protected def productDecoder(pageDetails: PageDetails = PageDetails()): Decoder[Product]

  /**
   * Sets exponential backoff timings used by http calls
   */
  protected val settings: RetrySettings = JsonShopScraper.defaultRetrySettings

  protected def scrapingAlgorithm()(implicit system: ActorSystem[Nothing]): Future[Try[List[Product]]]

  //TODO implement actor behavior
  def apply(): Behavior[Controller.Message] =
    Behaviors.setup { context =>
      implicit val system: ActorSystem[Nothing] = context.system

      Behaviors.receive[Controller.Message] { (context, message) =>
        message match {
          case Controller.ScrapData =>
            val result = scrapingAlgorithm()
            ???
        }
      }
    }


  /**
   * Replace DecodingFailure by empty string in case json path does not exist
   */
  implicit class ACursorOps(c: ACursor) {
    def asSafe[A](implicit decoder: Decoder[A]): Result[String] = c.as[A] match {
      case Left(_) => Right("")
      case Right(value) => Right(value.toString)
    }
  }

  protected def productListDecoder(implicit decoder: Decoder[Product]): Decoder[List[Product]] = (c: HCursor) => {
    c.value.asArray.get.map(_.as[Product])
      .foldLeft[Either[DecodingFailure, List[Product]]](Right(List.empty))((prev, curr) => curr match {
        case Left(failure) => throw failure
        case Right(value) => prev.map(value :: _)
      })
  }

  /**
   * @param decoder Function decoding json with some additional type B passed as argument
   * @param system Implicit ActorSystem
   * @tparam A Output type packed in List. Used by decoder to decode json
   * @tparam B Additional info in flow. E.g PageDetails
   * @return Generic flow used to decode json to list of elements of type A
   */
  protected def decoderListFlow[A, B](decoder: B => Decoder[A])(implicit system: ActorSystem[Nothing]) =
    Flow.fromFunction[(Json, B), Try[List[A]]] { jsonWithInfo =>
        val json = jsonWithInfo._1
        val additionalInfo = jsonWithInfo._2

        implicit val insideProductDecoder: Decoder[A] = decoder(additionalInfo)

        decode[List[A]](json.noSpaces) match {
          case Left(_) =>
            system.log.error(s"Parsing failure in $json")
            Failure(ParsingError)
          case Right(value) => Success(value)
        }
    }



  /**
   * @param focus Path to focused json. Result of successful parsing path MUST be different then unsuccessful parsing
   * @param settings Retry setting applied to RetryFlow
   * @param system Implicit ActorSystem
   * @tparam A Additional info in flow. E.g PageDetails
   * @return Flow returning possible json packed with additional info
   */
  protected def requestJson[A](focus: JsonPath = root, settings: RetrySettings = defaultRetrySettings)
                              (implicit system: ActorSystem[Nothing]):
  Flow[(HttpRequest, A), (Json, A), NotUsed] = {

    val mainFlow= Http().superPool[A]().map {
      case (Success(httpResponse), additionalInfo) =>
        httpResponse match {
          case HttpResponse(StatusCodes.OK, _, entity, _) =>
            val responseBody: String = Await.result(entity.toStrict(2.minutes), 2.minutes).data.utf8String
            // JSON parsing and path routing. Failed or incorrect JSON values are returned as Failure

            parse(responseBody).map(focus.json.getOption) match {
              case Left(_) => Failure(ParsingError)
              case Right(None) => Failure(NoSuchJsonPath)
              case Right(Some(json)) => Success((json, additionalInfo))
            }
          case HttpResponse(statusCode, _, _, _) =>
            Failure(HttpRequestNotOk(statusCode))
        }
      case (Failure(_), _) =>
        Failure(UnsuccessfulFetch)
    }

    RetryFlow.withBackoff(
      minBackoff = settings.minBackoff,
      maxBackoff = settings.maxBackoff,
      randomFactor = settings.randomFactor,
      maxRetries = settings.maxRetries,
      flow = mainFlow
    )(decideRetry = {
      case (packedRequest, Failure(exception)) =>
        system.log.info(s"Retrying http to json flow to ${packedRequest._1.uri}. Returned \"$exception\"")
        Some(packedRequest)
      case _ => None  // success case
    }).map {
      case Failure(_) => throw MaximumRetriesReached // stream failed after several retries
      case Success(json) => json
    }
  }

  /**
   * @param packedRequests Requests with PageDetails that will be used as Source
   * @param system Implicit ActorSystem
   * @return Future of possible list of products. Uses exponential backoff to retry fetching data in failed cases
   */
  def products(packedRequests: Iterable[(HttpRequest, PageDetails)], productProcessor: Flow[(Json, PageDetails), List[Product], NotUsed])(implicit system: ActorSystem[Nothing]):
  Future[List[Product]] =
    Source(packedRequests)
      .via(requestJson(root, settings))
      .via(productProcessor)
      .runWith(Sink.reduce((prev, curr) => curr ::: prev))
}

object JsonShopScraper {
  val defaultRetrySettings: RetrySettings = RetrySettings(
    minBackoff = 1.second, maxBackoff = 60.seconds, randomFactor = 0.1, maxRetries = 5
  )
}
