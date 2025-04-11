package com.telecom;

public class GetClassNameUtil {
    /**
     * 获取当前类的名称，并去掉所有的点号（.）。
     * @return 当前类的名称，去掉点号后的字符串。
     */
    public static String getCurrentClassNameWithoutDots() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        // 获取调用者的类名（即第三个元素）
        String className = stackTraceElements[2].getClassName();
        // 去掉类名中的所有点号
        String classNameWithoutDots = className.replace(".", "");
        return classNameWithoutDots;
    }

}
