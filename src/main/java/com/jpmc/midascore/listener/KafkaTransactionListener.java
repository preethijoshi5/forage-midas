package com.jpmc.midascore.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public KafkaTransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository
    ) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {

        UserRecord sender =
                userRepository.findById(transaction.getSenderId());

        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId());

        // validation
        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // call incentive API
        RestTemplate restTemplate = new RestTemplate();

        Incentive incentive =
                restTemplate.postForObject(
                        "http://localhost:8080/incentive",
                        transaction,
                        Incentive.class
                );

        float incentiveAmount = 0;

        if (incentive != null) {
            incentiveAmount = incentive.getAmount();
        }

        // update balances
        sender.setBalance(
                sender.getBalance() - transaction.getAmount()
        );

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount
        );

        // save updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        // save transaction
        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount()
                );

        transactionRecordRepository.save(record);

        // print wilbur balance
        if (sender.getName().equals("wilbur")) {

            System.out.println(
                    "WILBUR SENDER BALANCE = "
                            + sender.getBalance()
            );
        }
        if (recipient.getName().equals("wilbur")) {

    System.out.println(
            "WILBUR RECIPIENT BALANCE = "
                    + recipient.getBalance()
            );
        }
    }
}
    