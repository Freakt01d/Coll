"C:\Program Files\Java\jdk-25\bin\keytool" -import -alias sg-root-ca -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -file "SG UniPass Root CA 2016.cer" -storepass changeit

"C:\Program Files\Java\jdk-25\bin\keytool" -import -alias sg-intermediate-ca -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -file "SG-SubCA-SSLForwardProxy-GTS-SGGSC-PRD.cer" -storepass changeit



"C:\Program Files\Common Files\Oracle\Java\javapath\java.exe" -XshowSettings:properties -version 2>&1 | findstr "java.home"

"C:\Program Files\Java\jdk-25\bin\keytool" -list -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -storepass changeit -alias sg-root-ca

"C:\Program Files\Java\jdk-25\bin\java.exe" -Djavax.net.ssl.trustStore="C:\Program Files\Java\jdk-25\lib\security\cacerts" -Djavax.net.ssl.trustStorePassword=changeit -jar agent.jar -jnlpUrl https://cdp-jenkins-paas-rxd-94585.fr.world.socgen/computer/VP1DRXD000DEV1/jenkins-agent.jnlp -secret <your_secret>

"C:\Program Files\Java\jdk-25\bin\java.exe" -jar agent.jar -jnlpUrl https://cdp-jenkins-paas-rxd-94585.fr.world.socgen/computer/VP1DRXD000DEV1/jenkins-agent.jnlp -secret ef8166c450088f0fcd001c5a3b699f7b06657404095df190f0fab0e5a8a80 -cert @C:\sg-root.crt
