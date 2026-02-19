"C:\Program Files\Java\jdk-25\bin\keytool" -import -alias sg-root-ca -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -file "SG UniPass Root CA 2016.cer" -storepass changeit

"C:\Program Files\Java\jdk-25\bin\keytool" -import -alias sg-intermediate-ca -keystore "C:\Program Files\Java\jdk-25\lib\security\cacerts" -file "SG-SubCA-SSLForwardProxy-GTS-SGGSC-PRD.cer" -storepass changeit



"C:\Program Files\Common Files\Oracle\Java\javapath\java.exe" -XshowSettings:properties -version 2>&1 | findstr "java.home"
