package io.absurd.sdk;

/**
 * Options for creating a new queue.
 */
public class CreateQueueOptions {
    private final String queueName;

    private CreateQueueOptions(Builder builder) {
        this.queueName = builder.queueName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getQueueName() {
        return queueName;
    }

    public static class Builder {
        private String queueName;

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public CreateQueueOptions build() {
            if (queueName == null || queueName.trim().isEmpty()) {
                throw new IllegalStateException("queueName must be specified");
            }
            return new CreateQueueOptions(this);
        }
    }
}
