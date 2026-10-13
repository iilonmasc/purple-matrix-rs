/* purple-matrix-rs: the libpurple half.
 *
 * libpurple sees a protocol plugin written in C; everything Matrix lives in the Rust static library
 * this file is linked with (src/lib.rs), behind the handful of extern functions declared below.
 * Strings that cross come from the Rust side and go back to it with purple_matrix_string_free.
 *
 * Every account has a runtime thread on the Rust side. Login hands it the homeserver, the store
 * directory and the access token; joining and declining hand it a room id; the thread answers
 * through events — the connection, the rooms as chats, their members, lines and invitations —
 * which it announces with the wake callback registered here. The wake runs on the runtime thread, so all it does is schedule
 * matrix_drain on the main thread with purple_timeout_add — the one libpurple call that is safe
 * from another thread (glib's g_timeout_add is, and Adium's event loop says so) — and matrix_drain
 * collects the events there and tells libpurple. Close shuts the thread down and waits for it.
 *
 * The token is read from the password field and goes to the Rust side and nowhere else: never
 * into an account setting, so it stays where the client keeps it (Adium: the keychain). */

#include <glib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "account.h"
#include "accountopt.h"
#include "connection.h"
#include "conversation.h"
#include "debug.h"
#include "notify.h"
#include "plugin.h"
#include "prpl.h"
#include "server.h"
#include "status.h"
#include "util.h"
#include "version.h"

#define PRPL_ID "prpl-matrix"
#define OPT_HOME_SERVER "home_server"
#define OPT_PASSWORD_IS_TOKEN "password_is_token"

/* src/lib.rs */
typedef struct purple_matrix_account purple_matrix_account;

/* One event from the Rust side (src/lib.rs: CEvent); which fields a kind fills is listed there.
 * Strings are NULL where the kind has no value; the whole struct goes back with
 * purple_matrix_event_free. */
typedef struct {
	int kind;
	int reason;
	int64_t timestamp;
	char *room_id;
	char *event_id;
	char *user_id;
	char *name;
	char *old_name;
	char *text;
	char *topic;
	int member_count;
	char **member_ids;
	char **member_names;
} purple_matrix_event;

extern char *purple_matrix_version(void);
extern purple_matrix_account *purple_matrix_account_new(void (*wake)(void *ctx), void *ctx);
extern void purple_matrix_account_login(purple_matrix_account *account, const char *homeserver,
                                        const char *store_dir, const char *token);
extern void purple_matrix_account_join(purple_matrix_account *account, const char *room_id);
extern void purple_matrix_account_reject(purple_matrix_account *account, const char *room_id);
extern void purple_matrix_account_send(purple_matrix_account *account, const char *room_id, const char *body);
extern purple_matrix_event *purple_matrix_account_next_event(purple_matrix_account *account);
extern void purple_matrix_event_free(purple_matrix_event *event);
extern void purple_matrix_account_close(purple_matrix_account *account);
extern void purple_matrix_string_free(char *s);

/* purple_matrix_event's kinds (src/lib.rs: EVENT_*). An error's reason is a
 * PurpleConnectionError value, chosen on the Rust side. */
enum {
	MATRIX_EVENT_CONNECTED = 1,
	MATRIX_EVENT_ERROR = 2,
	MATRIX_EVENT_ROOM_JOINED = 3,
	MATRIX_EVENT_ROOM_LEFT = 4,
	MATRIX_EVENT_MEMBER_JOINED = 5,
	MATRIX_EVENT_MEMBER_RENAMED = 6,
	MATRIX_EVENT_MEMBER_LEFT = 7,
	MATRIX_EVENT_ROOM_NAMED = 8,
	MATRIX_EVENT_ROOM_TOPIC = 9,
	MATRIX_EVENT_MESSAGE = 10,
	MATRIX_EVENT_NOTICE = 11,
	MATRIX_EVENT_INVITED = 12,
	MATRIX_EVENT_JOIN_FAILED = 13,
	MATRIX_EVENT_LEAVE_FAILED = 14,
	MATRIX_EVENT_SEND_FAILED = 15
};

/* The one component of a chat: the room id. A bookmark carries it, an invite's data carries it,
 * and join_chat reads it. */
#define CHAT_ROOM_ID "room_id"

/* libpurple tells chats apart by an int; the room id's hash is it, as purple-matrix had it. */
static int matrix_chat_id(const char *room_id)
{
	return (int) g_str_hash(room_id);
}

static GHashTable *matrix_chat_components(const char *room_id)
{
	GHashTable *components = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
	g_hash_table_insert(components, g_strdup(CHAT_ROOM_ID), g_strdup(room_id));
	return components;
}

static const char *matrix_list_icon(PurpleAccount *account, PurpleBuddy *buddy)
{
	return "matrix";
}

static GList *matrix_status_types(PurpleAccount *account)
{
	GList *types = NULL;
	/* The ids are purple-matrix's, so an account made for it carries its saved status over
	 * without a warning. */
	types = g_list_append(types, purple_status_type_new_full(PURPLE_STATUS_AVAILABLE, "Online", NULL, TRUE, TRUE, FALSE));
	types = g_list_append(types, purple_status_type_new_full(PURPLE_STATUS_OFFLINE, "Offline", NULL, TRUE, TRUE, FALSE));
	return types;
}

/* A joined room: the chat opens under the room id, titled with the room's name, with its members
 * listed under the names they go by; the account's own member is the chat's nick, so a line under
 * that name is the account's own. */
static void matrix_room_joined(PurpleConnection *pc, const purple_matrix_event *ev)
{
	PurpleConversation *conv = serv_got_joined_chat(pc, matrix_chat_id(ev->room_id), ev->room_id);
	PurpleConvChat *chat = PURPLE_CONV_CHAT(conv);
	const char *own = purple_connection_get_display_name(pc);
	GList *names = NULL, *flags = NULL;
	int i;

	purple_conversation_set_title(conv, ev->text);
	for (i = 0; i < ev->member_count; i++) {
		names = g_list_append(names, ev->member_names[i]);
		flags = g_list_append(flags, GINT_TO_POINTER(PURPLE_CBFLAGS_NONE));
		if (own && g_strcmp0(ev->member_ids[i], own) == 0)
			purple_conv_chat_set_nick(chat, ev->member_names[i]);
	}
	purple_conv_chat_add_users(chat, names, NULL, flags, FALSE);
	g_list_free(names);
	g_list_free(flags);
	if (ev->topic)
		purple_conv_chat_set_topic(chat, NULL, ev->topic);
}

/* An event about an open chat; nothing to do when the chat is not open. */
static void matrix_chat_event(PurpleConnection *pc, const purple_matrix_event *ev)
{
	PurpleConversation *conv = purple_find_chat(pc, matrix_chat_id(ev->room_id));
	PurpleConvChat *chat;
	char *html, *text, *line;

	if (!conv)
		return;
	chat = PURPLE_CONV_CHAT(conv);
	switch (ev->kind) {
	case MATRIX_EVENT_MEMBER_JOINED:
		purple_conv_chat_add_user(chat, ev->name, NULL, PURPLE_CBFLAGS_NONE, TRUE);
		break;
	case MATRIX_EVENT_MEMBER_RENAMED:
		purple_conv_chat_rename_user(chat, ev->old_name, ev->name);
		break;
	case MATRIX_EVENT_MEMBER_LEFT:
		purple_conv_chat_remove_user(chat, ev->name, NULL);
		break;
	case MATRIX_EVENT_ROOM_NAMED:
		purple_conversation_set_title(conv, ev->text);
		break;
	case MATRIX_EVENT_ROOM_TOPIC:
		purple_conv_chat_set_topic(chat, ev->name, ev->topic);
		break;
	case MATRIX_EVENT_MESSAGE:
		/* The body is plain text; libpurple reads HTML. */
		html = g_markup_escape_text(ev->text, -1);
		if (g_strcmp0(ev->name, purple_conv_chat_get_nick(chat)) == 0) {
			/* The server's echo of what the account sent: the plugin wrote no line at send time,
			 * so this is the account's own line, once. */
			purple_conv_chat_write(chat, ev->name, html, PURPLE_MESSAGE_SEND, (time_t) ev->timestamp);
		} else {
			serv_got_chat_in(pc, matrix_chat_id(ev->room_id), ev->name, PURPLE_MESSAGE_RECV, html, (time_t) ev->timestamp);
		}
		g_free(html);
		break;
	case MATRIX_EVENT_SEND_FAILED:
		html = g_markup_escape_text(ev->text, -1);
		text = g_markup_escape_text(ev->name, -1);
		line = g_strdup_printf("Could not send \"%s\": %s", text, html);
		purple_conv_chat_write(chat, "", line, PURPLE_MESSAGE_ERROR, time(NULL));
		g_free(line);
		g_free(text);
		g_free(html);
		break;
	case MATRIX_EVENT_NOTICE:
		html = g_markup_escape_text(ev->text, -1);
		purple_conv_chat_write(chat, "", html, PURPLE_MESSAGE_SYSTEM, (time_t) ev->timestamp);
		g_free(html);
		break;
	}
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
		purple_matrix_event *ev;
		GHashTable *components;

		if (!account)
			return FALSE;
		ev = purple_matrix_account_next_event(account);
		if (!ev)
			return FALSE;

		switch (ev->kind) {
		case MATRIX_EVENT_CONNECTED:
			purple_debug_info("matrix", "signed in as %s\n", ev->text);
			purple_connection_set_display_name(pc, ev->text);
			purple_connection_set_state(pc, PURPLE_CONNECTED);
			break;
		case MATRIX_EVENT_ERROR:
			purple_debug_error("matrix", "connection failed (%d): %s\n", ev->reason, ev->text);
			purple_connection_error_reason(pc, (PurpleConnectionError) ev->reason, ev->text);
			break;
		case MATRIX_EVENT_ROOM_JOINED:
			matrix_room_joined(pc, ev);
			break;
		case MATRIX_EVENT_ROOM_LEFT:
			serv_got_chat_left(pc, matrix_chat_id(ev->room_id));
			break;
		case MATRIX_EVENT_INVITED:
			/* libpurple keeps the components and hands them to join_chat or reject_chat. */
			serv_got_chat_invite(pc, ev->text, ev->name, NULL, matrix_chat_components(ev->room_id));
			break;
		case MATRIX_EVENT_JOIN_FAILED:
			purple_debug_error("matrix", "joining %s failed: %s\n", ev->room_id, ev->text);
			components = matrix_chat_components(ev->room_id);
			purple_serv_got_join_chat_failed(pc, components);
			g_hash_table_destroy(components);
			purple_notify_error(pc, "Matrix", "The room could not be joined", ev->text);
			break;
		case MATRIX_EVENT_LEAVE_FAILED:
			purple_debug_error("matrix", "leaving %s failed: %s\n", ev->room_id, ev->text);
			purple_notify_error(pc, "Matrix", "The room could not be left", ev->text);
			break;
		default:
			matrix_chat_event(pc, ev);
			break;
		}
		purple_matrix_event_free(ev);

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

/* The chat dialog asks for one thing: the room id. */
static GList *matrix_chat_info(PurpleConnection *pc)
{
	struct proto_chat_entry *entry = g_new0(struct proto_chat_entry, 1);
	entry->label = "Room ID";
	entry->identifier = CHAT_ROOM_ID;
	entry->required = TRUE;
	return g_list_append(NULL, entry);
}

/* A chat's name is its room id, so a bookmark on the chat carries the id (decision 6). */
static GHashTable *matrix_chat_info_defaults(PurpleConnection *pc, const char *chat_name)
{
	GHashTable *components = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
	if (chat_name)
		g_hash_table_insert(components, g_strdup(CHAT_ROOM_ID), g_strdup(chat_name));
	return components;
}

static char *matrix_get_chat_name(GHashTable *components)
{
	return g_strdup(g_hash_table_lookup(components, CHAT_ROOM_ID));
}

/* Accepting an invite, opening a bookmark, the join dialog: the chat opens once the join is
 * synced, or the Rust side says why not. */
static void matrix_join_chat(PurpleConnection *pc, GHashTable *components)
{
	purple_matrix_account *handle = purple_connection_get_protocol_data(pc);
	const char *room_id = g_hash_table_lookup(components, CHAT_ROOM_ID);

	if (handle && room_id)
		purple_matrix_account_join(handle, room_id);
}

static void matrix_reject_chat(PurpleConnection *pc, GHashTable *components)
{
	purple_matrix_account *handle = purple_connection_get_protocol_data(pc);
	const char *room_id = g_hash_table_lookup(components, CHAT_ROOM_ID);

	if (handle && room_id)
		purple_matrix_account_reject(handle, room_id);
}

/* Typed into a chat: libpurple hands HTML, the room gets plain text. Nothing is written here
 * (libpurple does not echo chat messages itself); the line is the server's echo, once it
 * arrives. 0 means the message is on its way. */
static int matrix_chat_send(PurpleConnection *pc, int id, const char *message, PurpleMessageFlags flags)
{
	purple_matrix_account *handle = purple_connection_get_protocol_data(pc);
	PurpleConversation *conv = purple_find_chat(pc, id);
	char *plain;

	if (!handle || !conv)
		return -1;
	/* Strips the tags, turns <br> into newlines and resolves the entities. */
	plain = purple_markup_strip_html(message);
	purple_matrix_account_send(handle, purple_conversation_get_name(conv), plain);
	g_free(plain);
	return 0;
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
	prpl_info.chat_info = matrix_chat_info;
	prpl_info.chat_info_defaults = matrix_chat_info_defaults;
	prpl_info.get_chat_name = matrix_get_chat_name;
	prpl_info.join_chat = matrix_join_chat;
	prpl_info.reject_chat = matrix_reject_chat;
	prpl_info.chat_send = matrix_chat_send;

	prpl_info.protocol_options = g_list_append(NULL,
		purple_account_option_string_new("Home server URL", OPT_HOME_SERVER, "https://matrix.org"));
	prpl_info.protocol_options = g_list_append(prpl_info.protocol_options,
		purple_account_option_bool_new("The password is an access token", OPT_PASSWORD_IS_TOKEN, FALSE));
}

PURPLE_INIT_PLUGIN(matrix, matrix_plugin_init, info)
