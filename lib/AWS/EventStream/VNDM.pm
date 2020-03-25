package AWS::EventStream::VNDM;

use Moose;
use Digest::Crc32;
use IO::Scalar;
use Data::Dumper;

our $VERSION = '0.01';

has _one_meg => (
        is          => 'ro',
        isa         => 'Int',
        default     =>1048576 );
        

has _prelude_length => (
        is          => 'ro',
        isa         => 'Int',
        default     => 12
    );
        
has _prelude_checksum_length => (
        is          => 'ro',
        isa         => 'Int',
        default     => 4
    );
    
has _overhead_length => (
        is          => 'ro',
        isa         => 'Int',
        default     => 16
    );
        
sub decode {
    my $self = shift;
    my ($io_stream) = @_;
    # warn("HERE decode $io_stream");
    while (!$io_stream->eof){
       my $pos = $io_stream->getpos();
       my ($total_length,$header_length,$prelude_buffer) = $self->_prelude_check($io_stream);     
       $io_stream->setpos($pos);
       my ($message_buffer) =  $self->message_check($io_stream,$total_length,$header_length);
       my $sh = new IO::Scalar(\$message_buffer);

       my $headers = $self->get_headers($sh,$header_length);
       warn("headers=".Dumper($headers));
       my $bytes_read =  read $sh, my $message,$total_length-4;
       warn("message=$message");
      
    }
         
}

sub get_messsage {
      my $self = shift;
      my ($io_stream) = @_;
      return $io_stream
}

sub get_headers {
        my $self = shift;
        my ($io_stream,$header_length) = @_;
        
        my ($bytes_read) = read $io_stream, my $header_buffer, $header_length;
        
        my $return_header = {};
        my $json_decode_type = {'0'=>{pattern=>1,
                           length=>0,
                           type=>'bool_true'},
                     '1'=>{pattern=>0,
                           length=>0,
                           type=>'bool_false'},
                     '2'=>{pattern=>"c",
                           length=>1,
                           type=>'byte'},
                     '3'=>{pattern=>"s>",
                           length=> 2,
                           type=>'short'},
                     '4'=>{pattern=>"l>", 
                           length=>4,
                           type=>'integer'},
                     '5'=>{pattern=>"q>", 
                           length=>8,
                           type=>'long'},
                     '6'=>{pattern=>undef,
                           length=>undef,
                           type=>'bytes'},
                     '7'=>{pattern=>undef,
                           length=>undef,
                           type=>'string'},
                     '8'=>{pattern=>"q>", 
                           length=>8,
                           type=>'timestamp'},
                     '8'=>{pattern=>undef,
                           length=>16,
                           type=>'uuid'},};
        my $sh = new IO::Scalar(\$header_buffer);
        while (!$sh->eof) {
           my $pos = $sh->getpos;
           #print "my start=$pos\n";
           $bytes_read  = read $sh, my $key_len_b ,1; #get the first byte;
           my $key_len = unpack 'C' ,$key_len_b;
           #print "bytes_read=$bytes_read, key_len=$key_len\n";
           ($bytes_read)  = read $sh, my $key ,$key_len;
           #print "bytes_read=$bytes_read, key=$key\n";
           $bytes_read  = read $sh, my $type_b,1;
           my $type =  unpack 'C', $type_b;
           my $value_type = $json_decode_type->{$type};
           my $value;

           if ($value_type->{type} eq 'bool_true' || $value_type->{type} eq 'bool_false'){
                $value =  $value_type->{pattern};
            }
            else {
                my $value_length = $value_type->{length} ;
                unless($value_length) {
                  $bytes_read = read $sh, my $value_length_b,2;
                  $value_length = unpack 'S>', $value_length_b;
                }
                ($bytes_read) = read $sh, my $value_b,$value_length;
                
                if ($value_type->{pattern}){
                   $value = unpack $value_type->{pattern},$value_b;
                }                
                else {
                   $value = $value_b;     
                }
            }
            
            $return_header->{$key} = $value;
#print "bytes_read=$bytes_read, type=$type  7 in an array or string patern 'string' => [nil, nil, 7],\n";

         
 $pos = $sh->getpos;

#print "my pos=$pos\n";
}

        return $return_header;
}

sub message_check {
        my $self = shift;
        my ($io_stream,$total_length,$header_length) = @_;
        my $context_length = $total_length-$self->_overhead_length;      
        #warn("io_stream pos=".$io_stream->getpos());
        #warn("JSP message_check total_length=$total_length,header_length=$header_length context_length=$context_length");
       
       
 #       warn("here message_check again prelude_buffer=".$prelude_io->getpos);
        
        my $bytes_read =  read $io_stream, my $buffer, $total_length-4;  
        #warn("JSP message_check bytes_read=$bytes_read ");
        
        my $sh = new IO::Scalar(\$buffer);
        # my $p_sh = new IO::Scalar(\$prelude_buffer);
        $bytes_read =  read $sh, my $prelude, $self->_prelude_length;   
        $bytes_read =  read $sh, my $message, $total_length-$self->_prelude_length;   
        
         #warn("JSP message_check 2 bytes_read=$bytes_read ");
        $bytes_read = read $io_stream, my $messge_checksum,4;
       
        #warn("JSP message_check3 bytes_read=$bytes_read ");
        my ($checksum) = unpack 'N', $messge_checksum;
        # my $other_cs = $prelude_buffer.$message;
        my $crc = new Digest::Crc32();
         # warn("JSP message_check3 bytes_read=$other_cs ");
       #warn("JSP checksum=$checksum other=".$crc->strcrc32($buffer));
        
        
        if ($crc->strcrc32($buffer) != $checksum){
            die "Message checkum fails!";

         }
        return ($message);
}
sub _prelude_check {
        my $self = shift;
        my ($io_stream) = @_;
        my $bytes_read =  read $io_stream, my $prelude_crc, $self->_prelude_length;
        
        my $sh = new IO::Scalar(\$prelude_crc);
        $bytes_read = read $sh, my $prelude,  $self->_prelude_length()-$self->_prelude_checksum_length();
        $bytes_read = read $sh, my $prelude_checksum,  $self->_prelude_length;

        
        
        my ($checksum) = unpack 'N', $prelude_checksum;
       
        my $crc = new Digest::Crc32();

        if ($crc->strcrc32($prelude) != $checksum){

           die "Prelude checkum fails!";

         }
         my ($total_length, $header_length) =  unpack 'N*',$prelude;
         
        return  ($total_length, $header_length,$sh);
        
}

1;