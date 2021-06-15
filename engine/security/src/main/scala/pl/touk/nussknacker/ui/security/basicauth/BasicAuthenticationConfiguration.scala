package pl.touk.nussknacker.ui.security.basicauth

import com.typesafe.config.Config
import pl.touk.nussknacker.ui.security.api.{AuthenticationConfiguration, CachingHashesConfig}

import java.net.URI

case class BasicAuthenticationConfiguration(usersFile: URI,
                                            cachingHashes: Option[CachingHashesConfig]) extends AuthenticationConfiguration {

  override def method: String = BasicAuthenticationConfiguration.name

  def cachingHashesOrDefault: CachingHashesConfig = cachingHashes.getOrElse(CachingHashesConfig.defaultConfig)

  def implicitGrantEnabled: Boolean = false

  def idTokenNonceVerificationRequired: Boolean = false
}

object BasicAuthenticationConfiguration {

  import AuthenticationConfiguration._
  import pl.touk.nussknacker.engine.util.config.CustomFicusInstances._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  val name: String = "BasicAuth"

  def create(config: Config): BasicAuthenticationConfiguration =
    config.as[BasicAuthenticationConfiguration](authenticationConfigPath)
}