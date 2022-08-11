package com.demo.余弦相似度.model;

import java.util.Objects;

/**
 * 内容
 */
public class Item {

    /**
     * 词名
     */
    private String name;

    /**
     * 词频，也叫权重，用于词向量分析
     */
    private Float weight;

    public Item() {
    }


    public Item(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Float getWeight() {
        return weight;
    }

    public void setWeight(Float weight) {
        this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Item item = (Item) o;
        return getName().equals(item.getName()) && getWeight().equals(item.getWeight());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getWeight());
    }
}
