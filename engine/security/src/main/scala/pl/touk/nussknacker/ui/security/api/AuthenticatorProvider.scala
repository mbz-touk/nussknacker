package pl.touk.nussknacker.ui.security.api

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import pl.touk.nussknacker.engine.util.loader.ScalaServiceLoader
import pl.touk.nussknacker.ui.security.api.AuthenticatorFactory.AuthenticatorData
import pl.touk.nussknacker.ui.security.basicauth.BasicAuthenticatorFactory
import pl.touk.nussknacker.ui.security.oauth2.OAuth2AuthenticatorFactory
import sttp.client.{NothingT, SttpBackend}

import scala.concurrent.{ExecutionContext, Future}

object AuthenticatorProvider extends LazyLogging {
  def apply(config: Config, classLoader: ClassLoader, allCategories: List[String])(implicit ec: ExecutionContext, sttpBackend: SttpBackend[Future, Nothing, NothingT]): AuthenticatorData = {

    val loaded = ScalaServiceLoader.loadNamed[AuthenticatorFactory](config.getString(AuthenticationConfiguration.methodConfigPath), List(OAuth2AuthenticatorFactory(), BasicAuthenticatorFactory()))

    logger.info(s"Loaded authenticator method: $loaded.")

    loaded.createAuthenticator(config, classLoader, allCategories)
  }
}
