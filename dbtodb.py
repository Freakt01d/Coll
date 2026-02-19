"C:\Program Files\Java\jdk-25\bin\keytool" -import -alias sg-root-ca -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -file "SG UniPass Root CA 2016.cer" -storepass changeit

"C:\Program Files\Java\jdk-25\bin\keytool" -import -alias sg-intermediate-ca -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -file "SG-SubCA-SSLForwardProxy-GTS-SGGSC-PRD.cer" -storepass changeit



"C:\Program Files\Common Files\Oracle\Java\javapath\java.exe" -XshowSettings:properties -version 2>&1 | findstr "java.home"

"C:\Program Files\Java\jdk-25\bin\keytool" -list -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -storepass changeit -alias sg-root-ca

"C:\Program Files\Java\jdk-25\bin\java.exe" -Djavax.net.ssl.trustStore="C:\Program Files\Java\jdk-25\lib\security\cacerts" -Djavax.net.ssl.trustStorePassword=changeit -jar agent.jar -jnlpUrl https://cdp-jenkins-paas-rxd-94585.fr.world.socgen/computer/VP1DRXD000DEV1/jenkins-agent.jnlp -secret <your_secret>

