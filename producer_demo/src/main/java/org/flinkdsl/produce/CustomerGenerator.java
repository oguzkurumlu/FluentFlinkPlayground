package org.flinkdsl.produce;


import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class CustomerGenerator {
    private static final String[] FIRST = {"Ada","Can","Ece","Efe","Mina","Arya","Deniz","Lara","Emir","Defne"};
    private static final String[] LAST  = {"Yılmaz","Demir","Şahin","Çelik","Kaya","Yıldız","Yalçın","Aydın","Öztürk","Arslan"};

    public static Customer next() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String id = UUID.randomUUID().toString();
        String first = FIRST[rnd.nextInt(FIRST.length)];
        String last  = LAST[rnd.nextInt(LAST.length)];
        String email = (first + "." + last).toLowerCase(Locale.ROOT).replace("ı","i").replace("ş","s").replace("ğ","g").replace("ç","c").replace("ö","o").replace("ü","u") + "@example.com";
        long now = System.currentTimeMillis();
        return new Customer(id, first, last, email, now);
    }
}
