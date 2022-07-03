package com.spark.java.poc.solutions;

import com.google.common.primitives.Chars;

import java.util.ArrayList;

/**
 * Visa Online test
 */
public class VisaOnlineTestSolution {
    /**
     *  Need to check weather all the characters of "Note" is are available in the "Text" String.
     *
     * @param args
     */
    public static void main(String args[]) {
        String text ="The quick brown fox jumps over the lazy dog";
        String note = "visa";
        char arryChars[]=note.toCharArray();
        char arryTxt[] = text.toCharArray();
        int noteLen = note.length();
        ArrayList<Character> arryLis = new ArrayList<Character>();
        for(Character c : arryChars) {
            if(Chars.contains(arryTxt, c)){
                arryLis.add(c);
            }
        }
        boolean isConditionPassed = false;
        if (arryLis.size() == noteLen) {
            isConditionPassed = true;
        }

    }

    /**
     * Visa Company online test coding-
     * Need to get First non-repeated character
     *
     * @return
     */
    public int getNonRepeatedFirstChar(){
        int index = -1;
        String inputStr ="hackthegame";
        ArrayList<Character> arryChary = new ArrayList<Character>();
        for(char i :inputStr.toCharArray()){
            if ( inputStr.indexOf(i) == inputStr.lastIndexOf(i)) {
                System.out.println("First non-repeating character is: "+i);
                arryChary.add(i);
                //break;
            }
        }

        for(Character cha: arryChary){
            System.out.println("Character :"+cha);
            System.out.println("Index :"+inputStr.indexOf(cha));
            index = inputStr.indexOf(cha)+1;
            break;
        }

        System.out.println("Print Fist Index :"+index);
        return index;
    }
}
