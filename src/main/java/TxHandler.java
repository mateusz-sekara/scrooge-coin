import java.sql.Array;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TxHandler {
    private static final Predicate<Transaction> TRUTHY = (t -> true);
    private final UTXOPool utxoPool;

    /**
     * Creates a public ledger whose current UTXOPool (collection of unspent transaction outputs) is
     * {@code utxoPool}. This should make a copy of utxoPool by using the UTXOPool(UTXOPool uPool)
     * constructor.
     */
    public TxHandler(UTXOPool utxoPool) {
        this.utxoPool = new UTXOPool(utxoPool);
    }

    /**
     * @return true if:
     * (1) all outputs claimed by {@code tx} are in the current UTXO pool,
     * (2) the signatures on each input of {@code tx} are valid,
     * (3) no UTXO is claimed multiple times by {@code tx},
     * (4) all of {@code tx}s output values are non-negative, and
     * (5) the sum of {@code tx}s input values is greater than or equal to the sum of its output
     * values; and false otherwise.
     */
    public boolean isValidTx(Transaction tx) {
        return TRUTHY.and(this::allOutputsPresentInUTXO)
                .and(this::allSignaturesAreValid)
                .and(this::noUTXOisClaimedMultipleTimes)
                .and(this::allOutputsAreNonNegative)
                .and(this::inputsGreaterOrEqualOutputs)
                .test(tx);

    }

    private boolean inputsGreaterOrEqualOutputs(Transaction tx) {
        double inputSum = tx.getInputs()
                .stream()
                .map(this::findInUTXO)
                .mapToDouble(output -> output.value)
                .sum();

        double outputSum = tx.getOutputs()
                .stream()
                .mapToDouble(output -> output.value)
                .sum();

        return inputSum >= outputSum;
    }

    private boolean allOutputsAreNonNegative(Transaction tx) {
        return tx.getOutputs().stream().allMatch(output -> output.value > 0);
    }

    private boolean noUTXOisClaimedMultipleTimes(Transaction tx) {
        Set<UTXO> claimedUniqueUTXO = tx.getInputs()
                .stream()
                .map(input -> new UTXO(input.prevTxHash, input.outputIndex))
                .collect(Collectors.toSet());
        return claimedUniqueUTXO.size() == tx.numInputs();
    }

    private boolean allSignaturesAreValid(Transaction tx) {
        return IntStream.range(0, tx.numInputs())
                .mapToObj(index -> {
                    Transaction.Input input = tx.getInput(index);
                    Transaction.Output output = findInUTXO(input);
                    byte[] message = tx.getRawDataToSign(index);
                    return Crypto.verifySignature(output.address, message, input.signature);
                })
                .allMatch(x -> x);
    }

    public boolean allOutputsPresentInUTXO(Transaction tx) {
        return tx.getInputs()
                .stream()
                .allMatch(this::isPresentInUTXO);
    }

    private boolean isPresentInUTXO(Transaction.Input input) {
        UTXO utxo = new UTXO(input.prevTxHash, input.outputIndex);
        return utxoPool.contains(utxo);
    }

    private Transaction.Output findInUTXO(Transaction.Input input) {
        UTXO utxo = new UTXO(input.prevTxHash, input.outputIndex);
        return utxoPool.getTxOutput(utxo);
    }


    /**
     * Handles each epoch by receiving an unordered array of proposed transactions, checking each
     * transaction for correctness, returning a mutually valid array of accepted transactions, and
     * updating the current UTXO pool as appropriate.
     */
    public Transaction[] handleTxs(Transaction[] possibleTxs) {
        List<Transaction> transactions = outfilterDoubleSpends(Arrays.asList(possibleTxs));

        List<Transaction> firstCycle = validTxs(transactions);
        Set<byte[]> visitedOnes = firstCycle.stream().map(Transaction::getHash).collect(Collectors.toSet());

        List<Transaction> toBeDoubleChecked = transactions
                .stream()
                .filter(tx -> !visitedOnes.contains(tx.getHash()))
                .collect(Collectors.toList());

        List<Transaction> secondCycle = validTxs(toBeDoubleChecked);

        List<Transaction> output = new ArrayList<>(firstCycle);
        output.addAll(secondCycle);

        return output.toArray(new Transaction[0]);
    }

    private List<Transaction> validTxs(List<Transaction> txs) {
        List<Transaction> validTransactions = txs
                .stream()
                .filter(this::isValidTx)
                .collect(Collectors.toList());
        validTransactions.forEach(this::insertToUTXO);
        return validTransactions;
    }

    private List<Transaction> outfilterDoubleSpends(List<Transaction> transactions) {
        Set<InputTest> inputs = new HashSet<>();

        return transactions.stream()
                .sorted(Comparator.comparingInt(Transaction::numInputs).reversed())
                .filter(tx -> {
                    List<InputTest> uniqueSpendings = tx.getInputs()
                            .stream()
                            .map(InputTest::new)
                            .filter(input -> !inputs.contains(input))
                            .collect(Collectors.toList());

                    if (uniqueSpendings.size() != tx.numInputs()) {
                        return false;
                    }
                    inputs.addAll(uniqueSpendings);
                    return true;
                })
                .collect(Collectors.toList());
    }


    private void insertToUTXO(Transaction tx) {
        IntStream.range(0, tx.numInputs())
                .peek(index -> {
                    Transaction.Input input = tx.getInput(index);
                    Transaction.Output output = tx.getOutput(index);
                    if (output == null) {
                        return;
                    }

                    UTXO utxo = new UTXO(input.prevTxHash, input.outputIndex);
                    utxoPool.addUTXO(utxo, output);
                });
    }

    private static class InputTest {
        private final byte[] hash;
        private final int index;

        public InputTest(Transaction.Input input) {
            this(input.prevTxHash, input.outputIndex);
        }

        public InputTest(byte[] hash, int position) {
            this.hash = hash;
            this.index = position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InputTest inputTest = (InputTest) o;
            return index == inputTest.index && Arrays.equals(hash, inputTest.hash);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(index);
            result = 31 * result + Arrays.hashCode(hash);
            return result;
        }
    }
}
