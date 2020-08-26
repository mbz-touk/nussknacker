package pl.touk.nussknacker.ui.security.oauth2

import java.io.{File, FileInputStream}
import java.net.URI

import org.apache.commons.io.IOUtils
import com.typesafe.config.Config
import pl.touk.nussknacker.ui.security.api.AuthenticationConfiguration
import pl.touk.nussknacker.ui.security.api.AuthenticationMethod.AuthenticationMethod


case class OAuth2Configuration(method: AuthenticationMethod,
                               usersFile: String,
                               authorizeUri: URI,
                               clientSecret: String,
                               clientId: String,
                               profileUri: URI,
                               accessTokenUri: URI,
                               redirectUri: URI,
                               publicKeyFile: Option[String],
                               validateNonceOpt: Option[Boolean],
                               accessTokenParams: Map[String, String] = Map.empty,
                               authorizeParams: Map[String, String] = Map.empty,
                               headers: Map[String, String] = Map.empty,
                               authorizationHeader: String = "Authorization"
                              ) extends AuthenticationConfiguration {

  override def authorizeUrl: Option[URI] = Option({
    new URI(dispatch.url(authorizeUri.toString)
      .setQueryParameters((Map(
        "client_id" -> clientId,
        "redirect_uri" -> redirectUrl
      ) ++ authorizeParams).mapValues(v => Seq(v)))
      .url)
  })

  def redirectUrl: String = redirectUri.toString

  override def publicKey: Option[String] = {
    publicKeyFile.map(x => {
      val a = IOUtils.toString(new FileInputStream(new File(x)), "UTF-8")
      println(a)
      a
    })
  }

  override def validateNonce: Boolean = validateNonceOpt.getOrElse(super.validateNonce)
}

object OAuth2Configuration {
  import AuthenticationConfiguration._
  import pl.touk.nussknacker.engine.util.config.FicusReaders._
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._
  import net.ceedubs.ficus.readers.EnumerationReader._

  def create(config: Config): OAuth2Configuration = config.as[OAuth2Configuration](authenticationConfigPath)
}