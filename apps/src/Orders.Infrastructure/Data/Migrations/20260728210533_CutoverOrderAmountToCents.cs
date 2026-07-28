using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Orders.Infrastructure.Data.Migrations
{
    /// <inheritdoc />
    public partial class CutoverOrderAmountToCents : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // The old `amount` column is deliberately NOT dropped here - it's
            // reserved for a separate, later contract-phase migration, once
            // this cutover has been running against real traffic. Every row
            // was backfilled before this migration runs, so amount_cents can
            // safely become NOT NULL.
            migrationBuilder.AlterColumn<long>(
                name: "amount_cents",
                table: "orders",
                type: "bigint",
                nullable: false,
                defaultValue: 0L,
                oldClrType: typeof(long),
                oldType: "bigint",
                oldNullable: true);

            // The application stops writing to `amount` as of this deploy,
            // but the column still carries its original NOT NULL constraint
            // from before amount_cents existed - found by an integration
            // test failing on INSERT, not by reasoning about it in advance.
            // Relaxing it here (rather than in the same migration that drops
            // the column) keeps this phase's only real behavior change to
            // "stop reading/writing amount," with the column itself still
            // present and inspectable until the contract phase removes it.
            migrationBuilder.AlterColumn<decimal>(
                name: "amount",
                table: "orders",
                type: "numeric(18,2)",
                nullable: true,
                oldClrType: typeof(decimal),
                oldType: "numeric(18,2)");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<decimal>(
                name: "amount",
                table: "orders",
                type: "numeric(18,2)",
                nullable: false,
                defaultValue: 0m,
                oldClrType: typeof(decimal),
                oldType: "numeric(18,2)",
                oldNullable: true);

            migrationBuilder.AlterColumn<long>(
                name: "amount_cents",
                table: "orders",
                type: "bigint",
                nullable: true,
                oldClrType: typeof(long),
                oldType: "bigint");
        }
    }
}
