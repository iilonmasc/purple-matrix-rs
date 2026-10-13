/* purple-matrix-rs: the libpurple half.
 *
 * libpurple sees a protocol plugin written in C; everything Matrix lives in the Rust static library
 * this file is linked with (src/lib.rs), behind the handful of extern functions declared below.
 * Strings that cross come from the Rust side and go back to it with purple_matrix_string_free.
 *
 * What is here today is the skeleton that spec 001 builds on: the protocol registers, reports the
 * Rust side's version when it loads, and on login builds an SDK client with its stores and fails the
 * connection with what the Rust side said. */

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

/* src/lib.rs */
extern char *purple_matrix_version(void);
extern char *purple_matrix_client_probe(const char *homeserver, const char *store_dir);
extern void purple_matrix_string_free(char *s);

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

static void matrix_login(PurpleAccount *account)
{
	PurpleConnection *pc = purple_account_get_connection(account);
	const char *homeserver = purple_account_get_string(account, OPT_HOME_SERVER, "https://matrix.org");
	char *store = g_build_filename(purple_user_dir(), "matrix", purple_account_get_username(account), NULL);
	char *answer;

	purple_build_dir(store, 0700);
	answer = purple_matrix_client_probe(homeserver, store);
	purple_debug_info("matrix", "%s\n", answer);

	/* The skeleton goes no further than this: a client with stores, and an honest connection error. */
	purple_connection_error_reason(pc, PURPLE_CONNECTION_ERROR_OTHER_ERROR, answer);

	purple_matrix_string_free(answer);
	g_free(store);
}

static void matrix_close(PurpleConnection *pc)
{
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
}

PURPLE_INIT_PLUGIN(matrix, matrix_plugin_init, info)
