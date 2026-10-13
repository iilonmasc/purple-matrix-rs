/* purple-matrix-rs: the libpurple half.
 *
 * libpurple sees a protocol plugin written in C; everything Matrix lives in the Rust static library
 * this file is linked with (src/lib.rs), behind the handful of extern functions declared below.
 * Strings that cross come from the Rust side and go back to it with purple_matrix_string_free.
 *
 * Every account has a runtime thread on the Rust side. Login hands it the homeserver, the store
 * directory and the access token; the thread answers through events, which it announces with the
 * wake callback registered here. The wake runs on the runtime thread, so all it does is schedule
 * matrix_drain on the main thread with purple_timeout_add — the one libpurple call that is safe
 * from another thread (glib's g_timeout_add is, and Adium's event loop says so) — and matrix_drain
 * collects the events there and tells libpurple. Close shuts the thread down and waits for it.
 *
 * The token is read from the password field and goes to the Rust side and nowhere else: never
 * into an account setting, so it stays where the client keeps it (Adium: the keychain). */

#include <glib.h>
#include <string.h>

#include "account.h"
#include "accountopt.h"
#include "connection.h"
#include "debug.h"
#include "plugin.h"
#include "prpl.h"
#include "status.h"
#include "util.h"
#include "version.h"

#define PRPL_ID "prpl-matrix"
#define OPT_HOME_SERVER "home_server"
#define OPT_PASSWORD_IS_TOKEN "password_is_token"

/* src/lib.rs */
typedef struct purple_matrix_account purple_matrix_account;
extern char *purple_matrix_version(void);
extern purple_matrix_account *purple_matrix_account_new(void (*wake)(void *ctx), void *ctx);
extern void purple_matrix_account_login(purple_matrix_account *account, const char *homeserver,
                                        const char *store_dir, const char *token);
extern int purple_matrix_account_next_event(purple_matrix_account *account, int *reason, char **text);
extern void purple_matrix_account_close(purple_matrix_account *account);
extern void purple_matrix_string_free(char *s);

/* purple_matrix_account_next_event's answers (src/lib.rs: EVENT_*). An error's reason is a
 * PurpleConnectionError value, chosen on the Rust side. */
enum {
	MATRIX_EVENT_NONE = 0,
	MATRIX_EVENT_CONNECTED = 1,
	MATRIX_EVENT_ERROR = 2
};

static const char *matrix_list_icon(PurpleAccount *account, PurpleBuddy *buddy)
{
	return "matrix";
}

static GList *matrix_status_types(PurpleAccount *account)
{
	GList *types = NULL;
	types = g_list_append(types, purple_status_type_new_full(PURPLE_STATUS_AVAILABLE, NULL, NULL, TRUE, TRUE, FALSE));
	types = g_list_append(types, purple_status_type_new_full(PURPLE_STATUS_OFFLINE, NULL, NULL, TRUE, TRUE, FALSE));
	return types;
}

/* On the main thread: collect what the runtime thread left and tell libpurple. */
static gboolean matrix_drain(gpointer data)
{
	PurpleConnection *pc = data;

	/* The wake comes from the runtime thread, and close cannot take a scheduled one back; so the
	 * timer may fire for a connection that is already gone. */
	if (!PURPLE_CONNECTION_IS_VALID(pc))
		return FALSE;

	for (;;) {
		purple_matrix_account *account = purple_connection_get_protocol_data(pc);
		int reason = 0;
		char *text = NULL;
		int kind;

		if (!account)
			return FALSE;
		kind = purple_matrix_account_next_event(account, &reason, &text);
		if (kind == MATRIX_EVENT_NONE)
			return FALSE;

		if (kind == MATRIX_EVENT_CONNECTED) {
			purple_debug_info("matrix", "signed in as %s\n", text);
			purple_connection_set_display_name(pc, text);
			purple_connection_set_state(pc, PURPLE_CONNECTED);
		} else if (kind == MATRIX_EVENT_ERROR) {
			purple_debug_error("matrix", "connection failed (%d): %s\n", reason, text);
			purple_connection_error_reason(pc, (PurpleConnectionError) reason, text);
		}
		purple_matrix_string_free(text);

		/* An error tears the connection down, and with it the protocol data. */
		if (!PURPLE_CONNECTION_IS_VALID(pc))
			return FALSE;
	}
}

/* On the runtime thread: events wait, so collect them on the main thread. Nothing else of
 * libpurple is touched here. */
static void matrix_wake(void *ctx)
{
	purple_timeout_add(0, matrix_drain, ctx);
}

static void matrix_login(PurpleAccount *account)
{
	PurpleConnection *pc = purple_account_get_connection(account);
	const char *homeserver = purple_account_get_string(account, OPT_HOME_SERVER, "https://matrix.org");
	const char *password = purple_account_get_password(account);
	purple_matrix_account *handle;
	char *store;

	if (!purple_account_get_bool(account, OPT_PASSWORD_IS_TOKEN, FALSE)) {
		purple_connection_error_reason(pc, PURPLE_CONNECTION_ERROR_AUTHENTICATION_IMPOSSIBLE,
			"Signing in with a password is not there yet: put an access token in the password field "
			"and enable \"The password is an access token\" in the account's options.");
		return;
	}

	handle = purple_matrix_account_new(matrix_wake, pc);
	if (!handle) {
		purple_connection_error_reason(pc, PURPLE_CONNECTION_ERROR_OTHER_ERROR,
			"The account's runtime thread could not be started.");
		return;
	}
	purple_connection_set_protocol_data(pc, handle);
	purple_connection_set_state(pc, PURPLE_CONNECTING);
	purple_connection_update_progress(pc, "Checking the access token", 0, 2);

	store = g_build_filename(purple_user_dir(), "matrix", purple_account_get_username(account), NULL);
	purple_build_dir(store, 0700);
	purple_matrix_account_login(handle, homeserver, store, password ? password : "");
	g_free(store);
}

static void matrix_close(PurpleConnection *pc)
{
	purple_matrix_account *handle = purple_connection_get_protocol_data(pc);

	if (!handle)
		return;
	purple_connection_set_protocol_data(pc, NULL);
	/* Shuts the runtime thread down and waits for it; the client is dropped on its runtime. */
	purple_matrix_account_close(handle);
}

static gboolean matrix_plugin_load(PurplePlugin *plugin)
{
	char *version = purple_matrix_version();
	purple_debug_info("matrix", "loaded: %s\n", version);
	purple_matrix_string_free(version);
	return TRUE;
}

static PurplePluginProtocolInfo prpl_info;

static PurplePluginInfo info = {
	PURPLE_PLUGIN_MAGIC,
	PURPLE_MAJOR_VERSION,
	PURPLE_MINOR_VERSION,
	PURPLE_PLUGIN_PROTOCOL,
	NULL,                                     /* ui_requirement */
	0,                                        /* flags */
	NULL,                                     /* dependencies */
	PURPLE_PRIORITY_DEFAULT,
	PRPL_ID,                                  /* id */
	"Matrix",                                 /* name */
	"0.1.0",                                  /* version */
	"Matrix protocol plugin",                 /* summary */
	"Matrix on the matrix-rust-sdk, end-to-end encryption included",
	"the purple-matrix-rs authors",           /* author */
	"https://github.com/iilonmasc/purple-matrix-rs",
	matrix_plugin_load,                       /* load */
	NULL,                                     /* unload */
	NULL,                                     /* destroy */
	NULL,                                     /* ui_info */
	&prpl_info,                               /* extra_info */
	NULL,                                     /* prefs_info */
	NULL,                                     /* actions */
	NULL, NULL, NULL, NULL
};

static void matrix_plugin_init(PurplePlugin *plugin)
{
	memset(&prpl_info, 0, sizeof(prpl_info));
	prpl_info.struct_size = sizeof(prpl_info);
	prpl_info.options = OPT_PROTO_PASSWORD_OPTIONAL | OPT_PROTO_UNIQUE_CHATNAME | OPT_PROTO_CHAT_TOPIC;
	prpl_info.list_icon = matrix_list_icon;
	prpl_info.status_types = matrix_status_types;
	prpl_info.login = matrix_login;
	prpl_info.close = matrix_close;

	prpl_info.protocol_options = g_list_append(NULL,
		purple_account_option_string_new("Home server URL", OPT_HOME_SERVER, "https://matrix.org"));
	prpl_info.protocol_options = g_list_append(prpl_info.protocol_options,
		purple_account_option_bool_new("The password is an access token", OPT_PASSWORD_IS_TOKEN, FALSE));
}

PURPLE_INIT_PLUGIN(matrix, matrix_plugin_init, info)
