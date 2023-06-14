credentials += Credentials(Path.userHome / ".credentials-ci")

addSbtPlugin("com.eed3si9n"    % "sbt-assembly"    % "1.1.0")
addSbtPlugin("org.scoverage"   % "sbt-scoverage"   % "1.6.1")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.2") // 2.4.3 is incompatible with scala 2.11.8
addSbtPlugin("org.scalameta"   % "sbt-mdoc"        % "2.1.1")
addSbtPlugin("org.scalameta"   % "sbt-scalafmt"    % "2.4.2")
