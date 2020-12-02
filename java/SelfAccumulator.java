import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/29 20:35
 *   @Description :
 *
 */
public class SelfAccumulator extends AccumulatorV2<PersonInfo, PersonInfo> implements Serializable{

    private PersonInfo pi = new PersonInfo(1000, 1000);

    /**
     * 判断RDD中的每个分区中的累加器对象是否是初始值,需要与reset设置的值保持一致。
     * @return
     */
    @Override
    public boolean isZero() {
        return pi.getPersonCount() == 10 && pi.getAgeCount() == 10;
    }

    @Override
    public AccumulatorV2<PersonInfo, PersonInfo> copy() {
        SelfAccumulator accumulator = new SelfAccumulator();
        accumulator.pi = this.pi;
        return accumulator;
    }

    @Override
    public void reset() {
        pi = new PersonInfo(10, 10);
    }

    @Override
    public void add(PersonInfo v) {
        pi.setPersonCount(pi.getPersonCount() + v.getPersonCount());
        pi.setAgeCount(pi.getAgeCount() + v.getAgeCount());
    }

    @Override
    public void merge(AccumulatorV2<PersonInfo, PersonInfo> other) {
        SelfAccumulator sa = (SelfAccumulator) other;
        pi.setPersonCount(pi.getPersonCount() + sa.pi.getPersonCount());
        pi.setAgeCount(pi.getAgeCount() + sa.pi.getAgeCount());
    }

    @Override
    public PersonInfo value() {
        return pi;
    }
}

class PersonInfo implements Serializable{
    private int personCount;
    private int ageCount;

    public PersonInfo(int personCount, int ageCount) {
        this.personCount = personCount;
        this.ageCount = ageCount;
    }

    public int getPersonCount() {
        return personCount;
    }

    public void setPersonCount(int personCount) {
        this.personCount = personCount;
    }

    public int getAgeCount() {
        return ageCount;
    }

    public void setAgeCount(int ageCount) {
        this.ageCount = ageCount;
    }

    @Override
    public String toString() {
        return "PersonInfo{" +
                "personCount=" + personCount +
                ", ageCount=" + ageCount +
                '}';
    }
}
