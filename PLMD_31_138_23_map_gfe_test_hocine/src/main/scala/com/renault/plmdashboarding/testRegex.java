package com.renault.plmdashboarding;

import java.io.*;
import java.util.regex.*;

public class testRegex {

	private static Pattern pattern;
    private static Matcher matcher;

    public static String textRegCheck(String s) {
        pattern = Pattern.compile("^([a-zA-Z0-9]){2}$");
        matcher = pattern.matcher(s);
        if (matcher.find()) {
           return  "ok";
        }
        else {return "ko";}
		
		
    }
    
    
    
}