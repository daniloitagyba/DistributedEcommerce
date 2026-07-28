using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Orders.Infrastructure.Data.Migrations
{
    /// <inheritdoc />
    public partial class MakeInboxSourcePositionIndexNonUnique : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "ux_inbox_messages_source_position",
                table: "inbox_messages");

            migrationBuilder.CreateIndex(
                name: "ix_inbox_messages_source_position",
                table: "inbox_messages",
                columns: ["consumer_name", "topic", "partition", "offset"]);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "ix_inbox_messages_source_position",
                table: "inbox_messages");

            migrationBuilder.CreateIndex(
                name: "ux_inbox_messages_source_position",
                table: "inbox_messages",
                columns: ["consumer_name", "topic", "partition", "offset"],
                unique: true);
        }
    }
}
