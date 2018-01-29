package com.example.demo.configuration;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.FlatFileFormatException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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
    public Step slaveStep() {
        return stepBuilderFactory.get("slaveStep")
            .<Person,Person>chunk(2)
            .reader(csvReader(null))
            .writer(writer())
            .faultTolerant()
            .skipLimit(2)
            .skip(FlatFileParseException.class)
            .skip(FlatFileFormatException.class)
            .skip(IllegalArgumentException.class)
            .listener(skipListener())
            .retryLimit(1)
            .retry(IllegalArgumentException.class)
            .listener(stepExecution())
            .listener(chunkListener())
            .build();
    }

    @Bean
    public Step partitionStep() {
        return stepBuilderFactory.get("partitionStep")
            .partitioner("slaveStep", partitioner())
            .step(slaveStep())
            .taskExecutor(taskExecutor())
            .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(5);
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setQueueCapacity(5);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    public CustomMultiResourcePartitioner partitioner() {
        CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
        Resource[] resources = new Resource[] { new ClassPathResource("names0.csv"), new ClassPathResource("names1.csv") };
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    public JobExecutionListener jobListener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                System.out.println("Before executing job");
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                System.out.println("After executing job");
            }
        };
    }

    @Bean
    public ChunkListener chunkListener() {
        return new ChunkListener() {
            @Override
            public void beforeChunk(ChunkContext context) {
                context.setAttribute("timing",System.currentTimeMillis());
                System.out.println("Before chunk");
            }

            @Override
            public void afterChunk(ChunkContext context) {
                Long timing = System.currentTimeMillis() - (Long)context.getAttribute("timing");
                System.out.println("After chunk. millis = " + timing);
            }

            @Override
            public void afterChunkError(ChunkContext context) {
                System.out.println("A chunk finished with errors");
            }
        };
    }

    @Bean
    public StepExecutionListener stepExecution() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                System.out.println("Preparing to execute step");
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                System.out.println("Ending executing step");
                System.out.println(stepExecution.getSummary());
                return stepExecution.getExitStatus();
            }
        };
    }

    private SkipListener<? super Person, ? super Person> skipListener() {
        return new SkipListener<Person, Person>() {
            @Override
            public void onSkipInRead(Throwable t) {
                System.out.println("Skipped in read line: " + ((FlatFileParseException)t).getInput() );
            }

            @Override
            public void onSkipInWrite(Person person, Throwable t) {
                System.out.println("Skipped in write name: " + person.getName() );
            }

            @Override
            public void onSkipInProcess(Person item, Throwable t) {

            }
        };
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Person> csvReader(@Value("#{stepExecutionContext[fileName]}") String filename) {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[]{"name", "age"});

        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(personMapper());

        FlatFileItemReader<Person> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new ClassPathResource(filename));
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
                people.stream()
                    .map(Person::getName)
                    .filter("PEDRO"::equals)
                    .findAny().ifPresent(pedro -> {
                    System.out.println("Throwing IllegalArgumentException for PEDRO");
                    throw new IllegalArgumentException();
                });

                System.out.println(String.join(", ",people.stream().map(Person::getName).toArray(String[]::new)));
            }
        };
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("job1")
            .incrementer(new RunIdIncrementer())
            .start(partitionStep())
            .listener(jobListener())
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