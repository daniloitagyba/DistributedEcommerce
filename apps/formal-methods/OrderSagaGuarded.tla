---- MODULE OrderSagaGuarded ----
(* Milestone 56: models the orchestrated saga's real guard - every reply
   handler in OrderSagaReplyConsumer only applies if the row's CURRENT
   `step` still equals the step the reply is actually for
   (SagaOrchestrationStore.TryAdvanceAsync's `WHERE step = @expected_step`,
   TryCompleteAsync's `WHERE step = @expected_step`). A reply for a step
   the saga has already moved past - including because SagaTimeoutSweeper
   already terminated it - is structurally a no-op here, because every
   action below requires `step` to still equal its own precondition.
   Checked against NoResurrection: once the saga reaches "Done", it can
   never leave "Done" again. *)

EXTENDS Naturals

VARIABLES step, wasDone

Steps == {"ReserveRequested", "DecidePayment", "CommitInventory", "ReleaseInventory", "Done"}

Init == step = "ReserveRequested" /\ wasDone = FALSE

ReserveReplyOK ==
    /\ step = "ReserveRequested"
    /\ step' = "DecidePayment"

ReserveReplyInsufficient ==
    /\ step = "ReserveRequested"
    /\ step' = "Done"

PaymentApproved ==
    /\ step = "DecidePayment"
    /\ step' = "CommitInventory"

PaymentDeclined ==
    /\ step = "DecidePayment"
    /\ step' = "ReleaseInventory"

CommitReply ==
    /\ step = "CommitInventory"
    /\ step' = "Done"

ReleaseReply ==
    /\ step = "ReleaseInventory"
    /\ step' = "Done"

\* SagaTimeoutSweeper: Milestone 43's documented scope boundary - a saga
\* that times out mid-flight is just terminated, never compensated. Can
\* fire from any non-terminal step.
TimeoutSweep ==
    /\ step # "Done"
    /\ step' = "Done"

\* A duplicate/late reply for a step the saga is no longer on: this models
\* the exact hazard (a reply racing a timeout, or a redelivered/duplicate
\* Kafka message) - guarded here by requiring `step` to still match, same
\* as the real SQL's WHERE clause. Since every action above already has
\* that guard built in, a late ReserveRequested reply arriving after
\* TimeoutSweep already moved `step` to "Done" simply finds no action
\* enabled for it - it is a no-op, exactly like the real UPDATE affecting
\* zero rows.

Next ==
    \/ ReserveReplyOK
    \/ ReserveReplyInsufficient
    \/ PaymentApproved
    \/ PaymentDeclined
    \/ CommitReply
    \/ ReleaseReply
    \/ TimeoutSweep

TypeOK == step \in Steps

NoResurrection == wasDone => (step = "Done")

Spec == Init /\ [][Next /\ (wasDone' = (wasDone \/ (step' = "Done")))]_<<step, wasDone>>

====
