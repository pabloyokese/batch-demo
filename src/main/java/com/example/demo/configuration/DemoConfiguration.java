package com.example.demo.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.validation.BindException;

import java.util.List;

@Configuration
@EnableBatchProcessing
public class DemoConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
            .<Person,Person>chunk(2)
            .reader(csvReader())
            .writer(writer())
            .build();
    }

    @Bean
    public ItemReader<Person> csvReader() {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[]{"name", "age"});

        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(personMapper());

        FlatFileItemReader<Person> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new ClassPathResource("names.csv"));
        itemReader.setLineMapper(lineMapper);
        return itemReader;
    }

    @Bean
    public FieldSetMapper<Person> personMapper() {
        return new FieldSetMapper<Person>() {
            @Override
            public Person mapFieldSet(FieldSet fieldSet) throws BindException {
                return new Person(fieldSet.readString("name"),fieldSet.readString("age"));
            }
        };
    }

    @Bean
    public ItemWriter<Person> writer() {
        return new ItemWriter<Person>() {
            @Override
            public void write(List<? extends Person> people) throws Exception {
                System.out.println(String.join(", ",people.stream().map(Person::getName).toArray(String[]::new)));
            }
        };
    }

    @Bean
    public Job job(Step step1) throws Exception {
        return jobBuilderFactory.get("job1")
            .incrementer(new RunIdIncrementer())
            .start(step1)
            .build();
    }

    private class Person {
        private String name,age;

        public Person(String name, String age) {
            this.name = name;
            this.age = age;
        }

        public String getAge() {
            return age;
        }

        public String getName() {
            return name;
        }

        public void setAge(String age) {
            this.age = age;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}