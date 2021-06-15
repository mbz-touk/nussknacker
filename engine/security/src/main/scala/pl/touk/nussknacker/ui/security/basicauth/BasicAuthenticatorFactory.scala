package pl.touk.nussknacker.ui.security.basicauth

import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.directives.SecurityDirectives
import com.typesafe.config.Config
import pl.touk.nussknacker.ui.security.api.AuthenticatorFactory.{AuthenticatorData, LoggedUserAuth}
import pl.touk.nussknacker.ui.security.api.{AuthenticationConfiguration, AuthenticatorFactory}
import sttp.client.{NothingT, SttpBackend}

import scala.concurrent.{ExecutionContext, Future}

class BasicAuthenticatorFactory extends AuthenticatorFactory with Directives {

  def name: String = BasicAuthenticationConfiguration.name

  override def createAuthenticator(config: Config, classLoader: ClassLoader, allCategories: List[String])(implicit ec: ExecutionContext, sttpBackend: SttpBackend[Future, Nothing, NothingT]): AuthenticatorData = {
    val configuration = BasicAuthenticationConfiguration.create(config)
    AuthenticatorData(createDirective(configuration, allCategories), configuration)
  }

  def createDirective(config: BasicAuthenticationConfiguration, allCategories: List[String]): LoggedUserAuth =
    SecurityDirectives.authenticateBasicAsync(
      authenticator = BasicHttpAuthenticator(config, allCategories),
      realm = realm
    )
}

object BasicAuthenticatorFactory {
  def apply(): BasicAuthenticatorFactory = new BasicAuthenticatorFactory()
}