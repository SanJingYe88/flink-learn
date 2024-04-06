package it.com.util;

import java.util.Random;

public class RandomUtil {

    private static final Random RANDOM = new Random();

    /**
     * 生成一个0到120（不包括120）之间的double类型随机数。
     *
     * @return 0到120之间的随机数
     */
    public static double generateRandomDoubleUpTo120() {
        return Double.parseDouble(String.format("%.2f", RANDOM.nextDouble() * 120));
    }

    /**
     * 生成一个在60到120（包括60和120）之间的double类型随机数。
     *
     * @return 60到120之间的随机数
     */
    public static double generateRandomDoubleBetween60And120() {
        // 生成0到1之间的随机数
        double random = Math.random();
        // 映射到60到120的范围
        double scaledRandom = 60 + (random * (120 - 60));
        return Double.parseDouble(String.format("%.2f", scaledRandom));
    }

    public static int generateRandomId() {
        return new Random().nextInt(3);
    }

    public static String generateLoginEventType() {
        int i = new Random().nextInt(3);
        return i > 1 ? "success":"fail";
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            double randomNumber = generateRandomDoubleUpTo120();
            System.out.println("Random number: " + randomNumber);
        }
    }
}
