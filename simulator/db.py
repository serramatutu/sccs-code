from abc import ABC, abstractmethod
from collections import defaultdict, deque
import logging
from typing import List, Tuple, Dict, Set, Deque

from simulator.types import Transaction, TransactionType

logger = logging.getLogger("partition")

class BasePartition(ABC):
    def __init__(self, id: int) -> None:
        self.id = id

        # keep track of processing times
        self._submitted_at: Dict[Transaction, float] = {}
        self._started_at: Dict[Transaction, float] = {}
        self._finished_at: Dict[Transaction, float] = {}

        self.keys: Dict[int, int] = defaultdict(int)
        self._current_time = 0

        self._pending: Set[Transaction] = set()
        self._started: Set[Transaction] = set()

        # track the state of keys when transactions were created
        self._keystate: Dict[Transaction, int] = {}

        # track the values that GET transactions read
        self._read_values: Dict[Transaction, int] = {}

        self._logger = logger.getChild(str(id))

    def _transaction_start(self, transaction: Transaction) -> None:
        """Saves the state of a transaction's key, effectively making a snapshot of the database for it when this is called."""
        self._keystate[transaction] = self.keys[transaction.key]
        self._started.add(transaction)
        self._started_at[transaction] = self._current_time

        self._logger.debug(f"{self._current_time:.4f}: {transaction.id} started. read {transaction.key}->{self.keys[transaction.key]}")

    def _transaction_finish(self, transaction: Transaction) -> None:
        self._started.remove(transaction)
        self._finished_at[transaction] = self._current_time

        if transaction.type == TransactionType.GET:
            self._keystate[transaction] = self.keys[transaction.key]
            return

        if transaction.type == TransactionType.OVERWRITE:
            self.keys[transaction.key] = 0
        else:
            self.keys[transaction.key] = self._keystate[transaction] + 1

        self._logger.debug(f"{self._current_time:.4f}: {transaction.id} finished. wrote {transaction.key}->{self.keys[transaction.key]} ({transaction.type})")

    @abstractmethod
    def _start_transactions(self, transaction: Transaction) -> None:
        pass

    @property
    def is_finished(self):
        return len(self._submitted_at) == len(self._finished_at)

    def get_latency_for_transaction(self, transaction: Transaction) -> None:
        return self._finished_at[transaction] - transaction.submit_time

    def get_read_value_for_transaction(self, transaction: Transaction) -> int:
        assert transaction in self._finished_at
        return self._keystate[transaction]

    def get_read_values(self) -> Dict[Transaction, int]:
        values = {}
        for transaction in self._finished_at:
            if transaction.type == TransactionType.GET:
                values[transaction] = self.get_read_value_for_transaction(transaction)
        return values

    def get_latencies(self) -> Dict[Transaction, float]:
        return {
            transaction: self.get_latency_for_transaction(transaction)
            for transaction in self._finished_at
        }

    def clock(self, current_time: float) -> None:
        self._current_time = current_time
        self._start_transactions()
        
        # calculate which transactions should have finished based on execution time
        finished_transactions = set(t for t in self._started if self._current_time - self._started_at[t] >= t.execution_time)
        for transaction in finished_transactions:
            self._transaction_finish(transaction)

    def submit_transaction(self, transaction: Transaction) -> None:
        self._pending.add(transaction)
        self._submitted_at[transaction] = self._current_time


class ReferencePartition(BasePartition):
    """Partition implementation that executes everything serially."""

    def clock(self, current_time: float) -> None:
        self._current_time = current_time

    def submit_transaction(self, transaction: Transaction) -> None:
        self._submitted_at[transaction] = transaction
        self._transaction_start(transaction)
        self._transaction_finish(transaction)

    def _start_transactions(self, transaction: Transaction) -> None:
        pass


class EagerPartition(BasePartition):
    """Partition implementation that executes transactions eagerly without checking for current pending transactions."""
    def _start_transactions(self) -> None:
        pending_clone = set(self._pending)
        for transaction in pending_clone:
            self._pending.remove(transaction)
            self._transaction_start(transaction)


class DeferredPartition(BasePartition):
    """Partition implementation that defers transactions."""
    def __init__(self, 
        id: int, 
        max_defer_time: float = -1,
    ) -> None:
        super().__init__(id)

        self.max_defer_time = max_defer_time

        self._deferred_at: Dict[Transaction, Set[Transaction]] = {}

        self._depended_by: Dict[Transaction, Set[Transaction]] = defaultdict(set)
        self._depends_on: Dict[Transaction, Set[Transaction]] = defaultdict(set)

    def _transaction_defer(self, transaction: Transaction) -> bool:
        depends_on_pending = set(
            other for other in self._pending
            if other.key == transaction.key
            and self._submitted_at[other] < self._submitted_at[transaction]
        )
        depends_on_deferred = set(
            other for other in self._depends_on
            if other.key == transaction.key
        )
        depends_on_started = set(
            other for other in self._started 
            if other.key == transaction.key
        )
        depends_on = (
            depends_on_pending
                .union(depends_on_deferred)
                .union(depends_on_started)
        )

        if len(depends_on) == 0:
            return False

        self._deferred_at[transaction] = self._current_time

        depends_on_text = ", ".join(str(other.id) for other in depends_on)
        self._logger.debug(f"deferring {transaction.id} because it depends on {depends_on_text}")
        self._depends_on[transaction] = depends_on
        for t in depends_on:
            self._depended_by[t].add(transaction)
        
        return True

    def _start_transactions(self) -> None:
        # check new transactions
        pending_clone = set(self._pending)
        for transaction in pending_clone:
            self._pending.remove(transaction)
            if not self._transaction_defer(transaction):
                self._transaction_start(transaction)

        # check if deferred transactions can now run
        started: Set[Transaction] = set()
        for transaction, dependencies in self._depends_on.items():
            # whether to trigger transaction eagerly even though it still has dependencies to avoid latency
            trigger_eager = (
                self.max_defer_time > 0
                and self._current_time - self._deferred_at[transaction] > self.max_defer_time
            )

            # remove dependencies from future transactions since we're executing this eagerly
            if trigger_eager:
                for depended_by in self._depended_by[transaction]:
                    self._depends_on[depended_by].remove(transaction)

                del self._depended_by[transaction]

            if len(dependencies) == 0:
                started.add(transaction)
                self._transaction_start(transaction)

        for started_transaction in started:
            del self._depends_on[started_transaction]
                

    def _transaction_finish(self, transaction: Transaction) -> None:
        super()._transaction_finish(transaction)
        
        for depended_by in self._depended_by[transaction]:
            self._depends_on[depended_by].remove(transaction)
        del self._depended_by[transaction]
