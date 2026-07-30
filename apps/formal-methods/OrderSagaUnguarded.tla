---- MODULE OrderSagaUnguarded ----
(* Milestone 56: the counterfactual - what if OrderSagaReplyConsumer's
   handlers did NOT require the row's current `step` to match the reply's
   expected step before applying it? (e.g. an UPSERT that unconditionally
   writes the next step, instead of TryAdvanceAsync/TryCompleteAsync's
   guarded `UPDATE ... WHERE step = @expected_step`.) Same state machine
   as OrderSagaGuarded, plus one additional action, BuggyLateReserveReply,
   that applies a ReserveInventory reply's effect unconditionally -
   modeling a redelivered or simply-late reply arriving after the saga
   has already moved on (including after SagaTimeoutSweeper already
   marked it Done). Checked against the same NoResurrection invariant. *)

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

TimeoutSweep ==
    /\ step # "Done"
    /\ step' = "Done"

\* The counterfactual bug: no guard on the CURRENT step at all - this
\* fires regardless of what step the saga is actually on, exactly what a
\* naive "just apply the reply" handler without SQL's WHERE-clause guard
\* would do if a ReserveInventory reply is redelivered or simply arrives
\* late (e.g. after TimeoutSweep already completed the saga).
BuggyLateReserveReply ==
    /\ step' = "DecidePayment"

Next ==
    \/ ReserveReplyOK
    \/ ReserveReplyInsufficient
    \/ PaymentApproved
    \/ PaymentDeclined
    \/ CommitReply
    \/ ReleaseReply
    \/ TimeoutSweep
    \/ BuggyLateReserveReply

TypeOK == step \in Steps

NoResurrection == wasDone => (step = "Done")

Spec == Init /\ [][Next /\ (wasDone' = (wasDone \/ (step' = "Done")))]_<<step, wasDone>>

====
