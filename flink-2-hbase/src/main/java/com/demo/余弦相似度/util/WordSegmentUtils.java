package com.demo.余弦相似度.util;

import com.demo.余弦相似度.model.Item;
import com.google.common.collect.Lists;
import com.hankcs.hanlp.HanLP;
//import com.hankcs.hanlp.restful.HanLPClient;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

/**
 * HanLP分词工具
 */
public class WordSegmentUtils {

    /**
     * 分词
     *
     * @param sentence
     * @return
     */
    public static List<Item> segment(String sentence) {
        // 轻量级客户端 HanLPClient hanLPClient = new HanLPClient("https://www.hanlp.com/api", null, "zh", 3); // auth不填则匿名，zh中文，mul多语种
        // 1、【分词和词性标注】采用HanLP中文自然语言处理中标准分词进行分词
        List<Term> termList = HanLP.segment(sentence);

        // 上面控制台打印信息就是这里输出的
        System.out.println(termList.toString());

        // 2、重新封装到Word对象中（term.word代表分词后的词语，term.nature代表改词的词性）
        List<Item> list = Lists.newArrayList();
        for (Term term : termList) {
            list.add(new Item(term.word));
        }

        return list;
    }
}
